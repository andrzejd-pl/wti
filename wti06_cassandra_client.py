from cassandra.cluster import Cluster
from cassandra.query import dict_factory


class CassandraClient(object):
    def __init__(self, contact_points=None, port=9042, keyspace='user_ratings'):
        if contact_points is None:
            contact_points = ['127.0.0.1']

        self.cluster = Cluster(contact_points, port)
        self.session = self.cluster.connect()
        self.keyspace = keyspace
        self.create_keyspace()
        self.session.set_keyspace(self.keyspace)
        self.session.row_factory = dict_factory

    def create_keyspace(self):
        self.session.execute("""
            CREATE KEYSPACE IF NOT EXISTS """ + self.keyspace + """
            WITH replication = { 'class': 'SimpleStrategy', 'replication_factor': '1' }
        """)

    def create_table(self, table):
        self.session.execute("""
            CREATE TABLE IF NOT EXISTS """ + self.keyspace + """.""" + table + """ (
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
            genre_imax int,
            genre_thriller int,
            genre_crime int,
            genre_film_noir int,
            genre_adventure int,
            genre_documentary int,
            PRIMARY KEY(user_ratings_id)
            )
        """)

    def push_data_table(self, table, row):
        self.session.execute(
            """
                INSERT INTO """ + self.keyspace + """.""" + table + """ (user_ratings_id, user_id, movie_id, rating,
                        genre_animation, genre_musical, genre_drama, genre_war, genre_romance, genre_sci_fi,
                        genre_mystery, genre_short, genre_action, genre_western, genre_horror, genre_children,
                        genre_comedy, genre_fantasy, genre_imax, genre_thriller, genre_crime, genre_film_noir,
                        genre_adventure, genre_documentary)
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
                    %(genre_imax)s,
                    %(genre_thriller)s,
                    %(genre_crime)s,
                    %(genre_film_noir)s,
                    %(genre_adventure)s,
                    %(genre_documentary)s
                )
            """,
            row
        )

    def get_data_table(self, table):
        return self.session.execute("SELECT * FROM " + self.keyspace + "." + table + ";")

    def get_data_table_per_column_value(self, table, column, value):
        return self.session.execute(
            "SELECT * FROM " + self.keyspace + "." + table + " WHERE " + column + "=" + value + " ALLOW FILTERING;")

    def clear_table(self, table):
        self.session.execute("TRUNCATE " + self.keyspace + "." + table + ";")

    def delete_table(self, table):
        self.session.execute("DROP TABLE " + self.keyspace + "." + table + ";")


def main():
    table = "user_avg_rating"
    client = CassandraClient()

    # tworzenie tabeli
    client.create_table(table)

    # umieszczanie danych w tabeli
    # client.push_data_table(table, userId=1337, avgMovieRating=4.2)

    # pobieranie zawartości tabeli
    print(client.get_data_table(table))

    # czyszczenie zawartości tabeli
    client.clear_table(table)
    client.get_data_table(table)

    # usuwanie tabeli
    client.delete_table(table)


if __name__ == "__main__":
    main()
