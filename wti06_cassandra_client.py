from cassandra.cluster import Cluster
from cassandra.query import dict_factory


def create_keyspace(session, keyspace):
    session.execute("""
        CREATE KEYSPACE IF NOT EXISTS """ + keyspace + """
        WITH replication = { 'class': 'SimpleStrategy', 'replication_factor': '1' }
    """)


def create_table(session, keyspace, table):
    session.execute("""
        CREATE TABLE IF NOT EXISTS """ + keyspace + """.""" + table + """ (
        user_ratings_id int,
        user_id int,
        movie_id int,
        rating float,
        genre_animation int,
        genre_musical int,
        genre_drama int,
        genre_war int,
        genre_romance int,
        genre_sci_fi int,
        genre_mystery int,
        genre_short int,
        genre_action int,
        genre_western int,
        genre_horror int,
        genre_children int,
        genre_comedy int,
        genre_fantasy int,
        genre_iMAX int,
        genre_thriller int,
        genre_crime int,
        genre_film_noir int,
        genre_adventure int,
        genre_documentary int,
        PRIMARY KEY(user_ratings_id)
        )
    """)


def push_data_table(session, keyspace, table, row):
    session.execute(
        """
            INSERT INTO """ + keyspace + """.""" + table + """ (user_ratings_id, user_id, movie_id, rating, genre_animation, genre_musical,
                    genre_drama, genre_war, genre_romance, genre_sci_fi, genre_mystery, genre_short, genre_action,
                    genre_western, genre_horror, genre_children, genre_comedy, genre_fantasy, genre_iMAX,
                    genre_thriller, genre_crime, genre_film_noir, genre_adventure, genre_documentary)
            VALUES (
                %(user_ratings_id)s,
                %(user_id)s,
                %(movie_id)s,
                %(rating)s,
                %(genre_animation)s, 
                %(genre_musical)s,
                %(genre_drama)s,
                %(genre_war)s,
                %(genre_romance)s,
                %(genre_sci_fi)s,
                %(genre_mystery)s,
                %(genre_short)s,
                %(genre_action)s,
                %(genre_western)s,
                %(genre_horror)s,
                %(genre_children)s,
                %(genre_comedy)s,
                %(genre_fantasy)s,
                %(genre_iMAX)s,
                %(genre_thriller)s,
                %(genre_crime)s,
                %(genre_film_noir)s,
                %(genre_adventure)s,
                %(genre_documentary)s
            )
        """,
        row
    )


def get_data_table(session, keyspace, table):
    return session.execute("SELECT * FROM " + keyspace + "." + table + ";")


def get_data_table_per_column_value(session, keyspace, table, column, value):
    return session.execute("SELECT * FROM " + keyspace + "." + table + " WHERE " + column + "=" + value + " ALLOW FILTERING;")


def clear_table(session, keyspace, table):
    session.execute("TRUNCATE " + keyspace + "." + table + ";")


def delete_table(session, keyspace, table):
    session.execute("DROP TABLE " + keyspace + "." + table + ";")


if __name__ == "__main__":
    keyspace = "user_ratings"
    table = "user_avg_rating"
    # utworzenia połączenia z klastrem
    cluster = Cluster(['127.0.0.1'], port=9042)
    session = cluster.connect()
    # utworzenie nowego keyspace
    create_keyspace(session, keyspace)
    # ustawienie używanego keyspace w sesji
    session.set_keyspace(keyspace)
    # użycie dict_factory pozwala na zwracanie słowników
    # znanych z języka Python przy zapytaniach do bazy danych
    session.row_factory = dict_factory
    # tworzenie tabeli
    create_table(session, keyspace, table)
    # umieszczanie danych w tabeli
    push_data_table(session, keyspace, table, userId=1337, avgMovieRating=4.2)
    # pobieranie zawartości tabeli i wyświetlanie danych
    get_data_table(session, keyspace, table)
    # czyszczenie zawartości tabeli
    clear_table(session, keyspace, table)
    get_data_table(session, keyspace, table)
    # usuwanie tabeli
    delete_table(session, keyspace, table)
