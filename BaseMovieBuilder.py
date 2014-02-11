from os import environ

__author__ = 'mrkaiser'


#imports
from sqlalchemy import *
from sqlalchemy.sql import *
from multiprocessing import Pool
import os

database_url = os.environ.get('DATABASE_URL', 'mysql+mysqlconnector://user:pass@localhost/imdb_new')


def m_connect():
    #get db engine
    db = create_engine(os.environ.get('DATABASE_URL', 'mysql+mysqlconnector://user:pass@localhost/imdb_new'))
    #open connection
    connection = db.connect()
    #get metadata
    metadata = MetaData()
    return db, connection, metadata


def q_exec(query):
    db, connection, metadata = m_connect()
    returnable = connection.execute(query).fetchall()
    return returnable


def build_queries():
    db, connection, metadata = m_connect()
    #probably should use a dictionary
    queries = []
    #reflect all the tables
    metadata.reflect(bind=db)
    #build query using select expression builder
    #get table metadata
    movie_info_idx = metadata.tables['movie_info_idx']
    movie_info = metadata.tables['movie_info']
    name = metadata.tables['name']
    cast_info = metadata.tables['cast_info']
    title = metadata.tables['title']
    train_query = select([movie_info_idx.c.movie_id, movie_info_idx.c.info], and_(movie_info_idx.c.info_type_id == 112))
    #get all the results
    training_movies = connection.execute(train_query).fetchall()
    top_movies = list(zip(*training_movies))[0]
    #get the director for each movie
    dir_query = select([cast_info.c.id, cast_info.c.person_id, cast_info.c.movie_id],
                       and_(cast_info.c.role_id == 8, cast_info.c.movie_id.in_(top_movies),
                            cast_info.c.nr_order <= 2)).order_by("movie_id desc")
    queries.append(dir_query)
    #get the actor for each movie
    actors_query = select([cast_info.c.id, cast_info.c.person_id, cast_info.c.movie_id],
                          and_(cast_info.c.role_id == 1, cast_info.c.movie_id.in_(top_movies),
                               cast_info.c.nr_order == 1)).group_by(cast_info.c.person_id).order_by("movie_id desc")
    queries.append(actors_query)
    #get the actress for each movie
    actress_query = select([cast_info.c.id, cast_info.c.person_id, cast_info.c.movie_id],
                           and_(cast_info.c.role_id == 2, cast_info.c.movie_id.in_(top_movies),
                                cast_info.c.nr_order == 1)).group_by(cast_info.c.person_id).order_by("movie_id desc")
    queries.append(actress_query)
    #get the writer
    writer_query = select([cast_info.c.id, cast_info.c.person_id, cast_info.c.movie_id],
                          and_(cast_info.c.role_id == 4, cast_info.c.movie_id.in_(top_movies),
                               cast_info.c.nr_order == 1)).order_by("movie_id desc")
    queries.append(writer_query)
    #movie years

    #ratings
    ratings_query = select([movie_info_idx.c.info, movie_info_idx.c.movie_id],
                           and_(movie_info_idx.c.movie_id.in_(top_movies), movie_info_idx.c.info_type_id == 101))
    queries.append(ratings_query)
    #votes
    votes_query = select([movie_info_idx.c.info, movie_info_idx.c.movie_id],
                         and_(movie_info_idx.c.movie_id.in_(top_movies), movie_info_idx.c.info_type_id == 100))
    queries.append(votes_query)

    #years
    release_query = select([movie_info.c.info, movie_info.c.movie_id],
                           and_(movie_info.c.movie_id.in_(top_movies), movie_info.c.info_type_id == 16,
                                movie_info.c.info.like('%USA%')))
    queries.append(release_query)
    return queries


def build_from_queries(queries):
    p = Pool(5)
    query_results = p.map(q_exec, queries)
    p.close()
    p.join()
    #process the query_results
    return query_results


#Main
def main():
    queries = build_queries()
    build_from_queries(queries)


if __name__ == '__main__': main()