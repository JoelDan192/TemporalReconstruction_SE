import pandas as pd
import numpy as np

epbias_idx = pd.DataFrame.from_csv('EPbiasbyAnsCountRank.csv')
mx_count = epbias_idx.Ans_count.max()
epbias_idx = epbias_idx.append(pd.DataFrame(
        {'Ans_count':range(0,mx_count),'AnsRank':[0]*len(range(0,mx_count)),"EPbias":[0]*len(range(0,mx_count))}))

#There are deleted users among these votes. To Take into account only active users refer to  ExploratoryPlotsByGroups.ipynb
votes = pd.DataFrame.from_csv('AnsVotes_TSeries.csv', index_col=None)

votes['Norm_DRank'] = votes['DRank']/votes['Ans_count']
votes['Norm_Pos'] = votes['AnsRank']/votes['Ans_count']

import itertools
import sys

def idx_gen(votes):
    new_idx_start = votes.shape[0]
    return itertools.count(new_idx_start)

def merge_two_dicts(x, y):
    z = x.copy()
    z.update(y)
    return z

def break_ties(df):
    s_df = df.sort_values(by=['AnsRank','Votes'],ascending=[True,False])
    if sum(s_df.groupby(['AnsRank']).count().reset_index(drop=False).Votes>1)>0 :
        tot = df.shape[0]
        s_df.index=range(tot)
        fst_mn = s_df[s_df['AnsRank']==s_df['AnsRank'].shift(-1)].index
        penalties = np.squeeze(np.asarray(np.matrix([np.concatenate([np.zeros(int(e)+1),np.ones(tot-(int(e)+1))]) for e in fst_mn]).sum(axis=0)))
        penalties = pd.Series(penalties)
        s_df.loc[:,'AnsRank'] = s_df.loc[:,'AnsRank'] + penalties
        s_df.index = df.sort_values(by=['AnsRank','Score'],ascending=[True,False]).index
    return s_df

## Attributes that can be filled later

def pad_votes(df,a,q,aid_idx,index_gen):
    assert (df.AcceptedAnsId==int(df.AcceptedAnsId.iloc[0])).all(),'There can only be an Accepted answer per Qid-time'
    if aid_idx[q].size==0:
        n_ans = df.shape[0]
        aid_idx[q] = aid_idx[q].append(df)
        aid_idx[q].loc[:,'Accepted'] = aid_idx[q].AnsId==aid_idx[q].AcceptedAnsId
        aid_idx[q] = aid_idx[q].sort_values(by=['Accepted','Score'],ascending=[False,False])
        aid_idx[q].loc[:,'AnsRank'] = range(1,n_ans+1)
        aid_idx[q].loc[:,'Ans_count'] = n_ans

        by_ReScore = aid_idx[q].sort_values(by='ReScore',ascending=False)
        by_ReScore.loc[:,'ReRank'] = range(1,int(by_ReScore.shape[0])+1)
        by_SE = aid_idx[q].sort_values(by=['Accepted','Score'],ascending=[False,False])
        by_SE.loc[:,'SeRank'] = range(1,int(by_SE.shape[0])+1)
        aid_idx[q].loc[:,'DRank'] = by_SE['SeRank'] - by_ReScore['ReRank']
        aid_idx[q].loc[:,'EPbias'] = pd.merge(epbias_idx,aid_idx[q][['AnsRank','Ans_count']],how='inner',
                                              on=['AnsRank','Ans_count'],left_index=True)['EPbias']

    else:
        padding_aids = set(aid_idx[q].AnsId).difference(set(df.AnsId))
        df_padding_aids = aid_idx[q][aid_idx[q].AnsId.isin(padding_aids)]
        tot_ans = len(padding_aids) + len(df.AnsId)
        #True ranking (best reconstruction)
        aid_idx[q] = df.append(df_padding_aids)
        aid_idx[q].loc[:,'AcceptedAnsId'] = int(df.AcceptedAnsId.iloc[0])
        aid_idx[q].loc[:,'Accepted'] = aid_idx[q].AnsId==aid_idx[q].AcceptedAnsId
        assert (aid_idx[q].AcceptedAnsId==int(aid_idx[q].AcceptedAnsId.iloc[0])).all(), 'There can only be an Accepted answer per Qid-time'
        aid_idx[q] = aid_idx[q].sort_values(by=['Accepted','Score'],ascending=[False,False])
        #Calculating D-rank
        by_ReScore = aid_idx[q].sort_values(by='ReScore',ascending=False)
        by_ReScore.loc[:,'ReRank'] = range(1,int(by_ReScore.shape[0])+1)
        by_SE = aid_idx[q].sort_values(by=['Accepted','Score'],ascending=[False,False])
        by_SE.loc[:,'SeRank'] = range(1,int(by_SE.shape[0])+1)
        aid_idx[q].loc[:,'DRank'] = by_SE['SeRank'] - by_ReScore['ReRank']

        aid_idx[q].loc[:,'AnsRank'] = range(1,tot_ans+1)
        aid_idx[q].loc[:,'Ans_count'] = tot_ans
        aid_idx[q].loc[aid_idx[q].AnsId.isin(padding_aids),['Votes']] = 0
        aid_idx[q].loc[aid_idx[q].AnsId.isin(padding_aids),['Age']] = a
        aid_idx[q][aid_idx[q].AnsId.isin(padding_aids)].index = [index_gen.next() for i in range(len(padding_aids))]
        aid_idx[q].loc[:,'EPbias'] = pd.merge(epbias_idx,aid_idx[q][['AnsRank','Ans_count']],how='inner',
                                              on=['AnsRank','Ans_count'],left_index=True)['EPbias']
    return aid_idx[q]

get_idx = idx_gen(votes)
get_null_row = lambda df,ans_c,age,a_id,q_id: pd.DataFrame(
    {'QuestionId':[q_id],'AnsId':[a_id],'Age':[age-1],'Norm_Pos':[1],'Norm_DRank':[0.0],'Ans_count':[ans_c-1],
        'ReScore':[0.0],'AnsRank':[ans_c-1],'Votes':[0], 'EPbias':[0],
        'Score':[0],'Upvotes':[0],'Downvotes':[0]},index=[get_idx.next()])
#Votes:[0] etc will be shifted anyways
append_null_day = lambda df: get_null_row(df,df.sort_values(by='Age').Ans_count.iloc[0],int(df['Age'].min()),df.AnsId.iloc[0],df.QuestionId.iloc[0]).append(df)
votes = votes.groupby(['AnsId']).apply(append_null_day).reset_index(level=[0],drop=True)
votes.loc[:,'Age'] = votes['Age'] + 1

atts_sft = ['Score','EPbias','QuestionId','Age','Norm_Pos','Norm_DRank','Ans_count','ReScore','AnsRank','AcceptedAnsId']
select = lambda df,one_vote=True: df[atts_sft + ['Votes','Upvotes','Downvotes']] if one_vote else df[atts_sft]
shift = lambda df,att : df[att].shift(-1)#.ffill()
shift_select = lambda df: select(df) if df.shape[0]==1 else pd.concat([select(df,one_vote=False),shift(df,'Votes'),shift(df,'Upvotes'),shift(df,'Downvotes')],axis=1)
shift_votes = lambda df: shift_select(df.sort_values(by='Age'))

votes = votes.groupby(['AnsId']).apply(shift_votes).reset_index(level=[0],drop=False)
votes = votes[(votes.Upvotes.notnull())|(votes.Downvotes.notnull())|(votes.Votes.notnull())] #drop last registered day for each ans

votes.loc[:,'AcceptedAnsId']=votes['AcceptedAnsId'].fillna(-1)
def clean_acc(df):
    if df[df.AcceptedAnsId!=-1].size==0 or df[df.AcceptedAnsId==-1].size==0:
        return df
    df.loc[df.AcceptedAnsId==-1,'AcceptedAnsId'] = df[df.AcceptedAnsId!=-1].AcceptedAnsId.iloc[0]
    return df
votes = votes.groupby(['QuestionId','Age']).apply(clean_acc).reset_index(drop=True)

from collections import defaultdict
#VERY expensive transformation
padded_votes = pd.DataFrame()
genidx = idx_gen(votes)
ans_index = defaultdict(lambda: pd.DataFrame()) #Qid,AnsId,Position
for q,g_q in votes.groupby(['QuestionId']):
    for a,g_a in g_q.sort_values(by='Age').groupby(['Age']):
        padded_votes = padded_votes.append(pad_votes(g_a,a,q,ans_index,genidx))

votes = padded_votes
votes.loc[:,"Norm_DRank"] = votes['DRank']/votes['Ans_count']
votes.loc[:,"Norm_Pos"] = votes['AnsRank']/votes['Ans_count']
votes.loc[:,'Norm_Pos_2'] = np.square(votes['Norm_Pos'])

votes.to_csv('VotesRaw.csv')
