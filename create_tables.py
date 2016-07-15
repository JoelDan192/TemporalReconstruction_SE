import sqlite3 as lite
import sys


def gen_table(schema=None,query=None,fname=None):
    conn = None
    try:
        conn = lite.connect('so-dump.db')
        cursor = conn.execute(query)
        with open(fname,'w') as f:
            f.write(schema+"\n")
            for row in cursor:
                f.write(','.join([str(e) if e else '' for e in row])+"\n")

    except lite.Error, e:
        print "Error %s:" %(e.args[0])
        sys.exit(1)
    finally:
        if conn:
            conn.close()

if __name__ == "__main__":
    query_voteans = """select v.Id,v.CreationDate,p.CreationDate,v.VoteTypeId,p.Id,p.ParentId,p.WordCount from
        (select * from votes where VoteTypeId=2 or VoteTypeId=3) as v,
        (select Id, ParentId, CreationDate, sum(length(Body) - length(replace(Body, ' ', ''))+1)
        as WordCount from posts where posts.PostTypeId=2 GROUP BY Id) as p where v.PostId=p.Id;"""
    schema_voteans = """VoteId,VoteCreation,AnsCreation,VoteType,AnsId,QuestionId,AnsWordCount"""
    name_voteans = 'votes-answers.csv'
    gen_table(schema_voteans,query_voteans,name_voteans)

    query_question = """select pv.Id, pv.CreationDate, pv.LastActivityDate, pv.AcceptedAnswerId, av.VoteCreation, pv.Score, pv.Votes FROM
    (select * from (select * from posts where PostTypeId=1) as p ,(select count(votes.Id) as Votes, votes.PostId from votes GROUP BY votes.PostId) as v where p.Id=v.PostId) as pv LEFT JOIN
    (select a.Id as Id,v.CreationDate as VoteCreation from (select Id,CreationDate from posts where PostTypeId=2) as a, (select CreationDate,PostId from votes where votes.VoteTypeId=1) as v where a.Id=v.PostId) as av
    ON av.Id=pv.AcceptedAnswerId;"""
    schema_question = """QuestionId,QuestionCreation,QuestionLastActivity,AcceptedAnsId,AcceptedDate,QScore,QVotes"""
    name_question = 'question_simple.csv'
    gen_table(schema_question,query_question,name_question)



    query_votes = """select pv.Id, pv.CreationDate, pv.LastActivityDate, pv.AcceptedAnswerId, av.VoteCreation, pv.VoteTypeId,pv.CreationDate FROM
    (select p.Id,p.CreationDate,p.LastActivityDate,p.AcceptedAnswerId,v.VoteTypeId,v.CreationDate from (select * from posts where PostTypeId=1) as p ,(select VoteTypeId,CreationDate,PostId from votes where VoteTypeId=2 or VoteTypeId=3) as v where p.Id=v.PostId) as pv LEFT JOIN
    (select a.Id as Id,v.CreationDate as VoteCreation from (select Id,CreationDate from posts where PostTypeId=2) as a, (select CreationDate,PostId from votes where votes.VoteTypeId=1) as v where a.Id=v.PostId) as av
    ON av.Id=pv.AcceptedAnswerId;"""
    schema_votes = """QuestionId,QuestionCreation,QuestionLastActivity,AcceptedAnsId,AcceptedDate,VoteType,QVoteCreation"""
    name_votes = 'question_votes.csv'
    gen_table(schema_votes,query_votes,name_votes)
