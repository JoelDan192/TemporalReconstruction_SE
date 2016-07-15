import pandas as pd
questions = pd.DataFrame.from_csv('question_simple.csv', index_col=None)
a_questions = pd.DataFrame.from_csv('question_votes.csv', index_col=None)
get_votes_qv = lambda df: pd.Series((df.VoteType==2).cumsum() + (df.VoteType==3).cumsum(),name='QVotes')
get_score_qv = lambda df: pd.Series((df.VoteType==2).cumsum() - (df.VoteType==3).cumsum(),name='QScore')

predictors_qvotes = ['QuestionId','QuestionCreation','QuestionLastActivity','AcceptedAnsId','AcceptedDate','QVoteCreation']
f_q = lambda df: pd.concat([df[cname] for cname in df.columns.values.tolist() if cname in predictors_qvotes]+[get_score_qv(df),get_votes_qv(df)],axis=1)
a_questions = a_questions.sort_values(by='QVoteCreation').groupby(['QuestionId']).apply(f_q)

a_votes = pd.DataFrame.from_csv('votes-answers.csv', index_col=None)
a_votes = pd.merge(a_votes, a_questions, how='inner', on=['QuestionId'],suffixes=['_v', '_q'])

predictors_raw_votans =['VoteId','VoteCreation','AnsCreation','VoteType','AnsId','QuestionId','AnsWordCount','QuestionCreation','AcceptedAnsId','AcceptedDate']

valid_qavotes = lambda df: df[df.VoteCreation>=df.QVoteCreation]
#Use twice valid_qavotes, could use once to improve efficiency, but check correctness of index selection
get_max_qv = lambda df: valid_qavotes(df).loc[valid_qavotes(df).QVotes.idxmax(),['QScore','QVotes']].squeeze()
get_latest_qv = lambda df : pd.Series([0,0],index=['QScore','QVotes']) if not (df.VoteCreation>=df.QVoteCreation).any() else get_max_qv(df)
get_head = lambda df: [df[cname].iloc[0] for cname in df.columns.values.tolist() if cname in predictors_raw_votans]
get_qv = lambda df : pd.Series(get_head(df),index=predictors_raw_votans).append(get_latest_qv(df)).to_frame()

a_votes = a_votes.sort_values(by='VoteCreation').groupby(['VoteId']).apply(get_qv).unstack(level=-1).reset_index(level=[0],drop=True)
a_votes.drop(a_votes.columns[[0]], axis=1, inplace=True)
a_votes.columns = a_votes.columns.droplevel()

date_placeholder = '2016-07-20T00:00:00.000' #Date After Data Set Collection
#a_votes.loc[a_votes.AcceptedDate == 'None','AcceptedDate'] = pd.to_datetime(date_placeholder)
a_votes['AcceptedDate'].fillna(pd.to_datetime(date_placeholder),inplace=True)
a_votes['AcceptedAge'] = (pd.to_datetime(a_votes.AcceptedDate,format='%Y-%m-%d %H:%M:%S.%f')
                  -pd.to_datetime(a_votes.QuestionCreation,format='%Y-%m-%d %H:%M:%S.%f')).apply(lambda x: x.astype('timedelta64[D]').item().days)
a_votes['AcceptedAge'] = a_votes['AcceptedAge'] + 1

a_votes.loc[a_votes.AcceptedDate == pd.to_datetime(date_placeholder), 'AcceptedAge'] = -1
a_votes['Age'] = (pd.to_datetime(a_votes.VoteCreation,format='%Y-%m-%d %H:%M:%S.%f')
                  -pd.to_datetime(a_votes.QuestionCreation,format='%Y-%m-%d %H:%M:%S.%f')).apply(lambda x: x.astype('timedelta64[D]').item().days)
a_votes['Age'] = a_votes['Age'] + 1

a_votes.drop(a_votes.columns[[0, 1, 6, 8]], axis=1, inplace=True)

get_score = lambda df: sum(df.VoteType==2) - sum(df.VoteType==3)
get_votes = lambda df: sum(df.VoteType==2) + sum(df.VoteType==3)

predictors = ['QuestionId','AnsWordCount','AcceptedAnsId','AcceptedAge','QScore',
              'QVotes','Score','Votes','Upvotes','Downvotes']
f = lambda df: pd.Series([df.QuestionId.iloc[0],df.AnsWordCount.iloc[0],df.AcceptedAnsId.iloc[0],df.AcceptedAge.iloc[0],
                          df.QScore.iloc[0],df.QVotes.iloc[0],get_score(df),get_votes(df),sum(df.VoteType==2),sum(df.VoteType==3)],index = predictors)
a_groups = a_votes.sort_values(by='Age').groupby(['AnsId','Age']).apply(f)
a_groups = a_groups.reset_index(level=[0,1],drop=False)

cum_votes = lambda df: pd.Series(df['Votes'].cumsum(),name='CumVotes')
cum_score = lambda df: pd.Series(df['Score'].cumsum(),name='CumScore')

get_cumulative =lambda df: pd.concat([df[cname] for cname in df.columns.values.tolist()] + [cum_votes(df),cum_score(df)],axis=1)
ff = lambda df: get_cumulative(df.sort_values(by='Age'))
a_groups_c = a_groups.groupby(['AnsId']).apply(ff).reset_index(level=[0],drop=True)

prior_quality = float(a_groups_c['Upvotes'].sum())/(a_groups_c['Upvotes'].sum() + a_groups_c['Downvotes'].sum())
a_groups_c['ReScore'] = (a_groups_c['CumScore']+prior_quality)/(a_groups_c['CumVotes']+1.0)
a_groups_c['QReScore'] = a_groups_c['QScore']/(a_groups_c['QVotes']+1.0)

votes_com_f = a_groups_c

from itertools import izip
def rank_ans(df,score_only,re_score):
    rk_name = "ReScore_rank" if re_score else "AnsRank"
    def rank_iter():
        cache = {}
        accepted = 0
        for row in df.itertuples():
            if re_score:
                cache[row.AnsId] = row.ReScore
            else :
                cache[row.AnsId] = row.Score
            # rank, nb_ans
            if (not score_only) and row.AcceptedAge>-1 and (row.AnsId == row.AcceptedAnsId) and row.Age >=row.AcceptedAge:
                accepted = 1
                if row.AnsId in cache:
                    del cache[row.AnsId]
                yield (1,len(cache)+accepted,row.Index)
            else :
                rank = sorted(cache, key= lambda k:cache[k],reverse=True).index(row.AnsId) + 1 + accepted
                yield (rank,len(cache)+accepted,row.Index)

    ranks, ans_counts, indices = izip(*list(rank_iter())) #TODO: optimize for the future
    return [pd.Series(ranks,name=rk_name, index=indices), pd.Series(ans_counts,name="Ans_count", index=indices)]

predictors = ['QuestionId','AnsId','AnsWordCount','AcceptedAnsId','Age',
              'Score','Votes','Upvotes','Downvotes','CumScore','CumVotes','QScore'
              ,'QVotes','ReScore','QReScore','AnsRank','ReScore_rank']
get_ranks = lambda df,score_only=False,re_score=False: pd.concat(
    [df[cname] for cname in df.columns.values.tolist() if cname in predictors] + rank_ans(df,score_only,re_score),axis=1)
sort_age_score = lambda df: df.sort_values(by=['Age','Score'],ascending=[True,False])

votes_com_f = votes_com_f.groupby(['QuestionId']).apply(
    lambda df: get_ranks(sort_age_score(df))).reset_index(drop=True)
votes_com_f = votes_com_f.groupby(['QuestionId']).apply(
    lambda df: get_ranks(sort_age_score(df),score_only=True,re_score=True)).reset_index(drop=True)

votes_com_f['Pbias'] = 1.0/votes_com_f['AnsRank']
votes_com_f['DRank'] = votes_com_f['AnsRank'] - votes_com_f['ReScore_rank']

#AnsRank and Ans_count define unique EPbias
sum_by_rank = lambda df: df.groupby('AnsRank').apply(
    lambda df: pd.Series([df.Votes.sum()],name='EPbias').to_frame()).unstack(level=-1).reset_index(level=0,drop=False)
get_ratio = lambda df: sum_by_rank(df).EPbias/(sum_by_rank(df).EPbias.sum())
ratio_per_rank = lambda df: pd.concat([sum_by_rank(df).AnsRank, get_ratio(df)],axis=1)
get_position_bias = lambda df: pd.merge(df,ratio_per_rank(df),how='inner',on=['AnsRank'])

votes = votes_com_f.groupby(['Ans_count']).apply(get_position_bias).reset_index(level=[0,1],drop=True)
votes.columns.values[-1] = "EPbias"

test_epbias = votes.groupby(['Ans_count','AnsRank']).first().reset_index(
    level=[0,1],drop=False)[['Ans_count','AnsRank','EPbias']]
test_epbias.to_csv('EPbiasbyAnsCountRank.csv')

votes.to_csv(path_or_buf='AnsVotes_TSeries.csv')
