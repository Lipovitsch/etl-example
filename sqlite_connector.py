from sqlite3 import connect

class SQLiteConnector:
    def __init__(self):
        self._connection = connect('tracks.db')
        self._cursor = self._connection.cursor()
        self._cursor.execute(
            '''CREATE TABLE IF NOT EXISTS unique_tracks (
                performance_id text
                , track_id text
                , artist text
                , track text)''')
        self._cursor.execute(
            '''CREATE TABLE IF NOT EXISTS triplets_sample (
                user_id text
                , track_id text
                , play_date text)''')

    def insert_data_tuple_to_db(self, table_name, data_tuple_list):
        data_tuple_length = len(data_tuple_list[0])
        param_string = ','.join('?' * data_tuple_length)
        query = f'INSERT INTO {table_name} VALUES ({param_string})'
        try:
            self._cursor.executemany(query, data_tuple_list)
            self._connection.commit()
        except Exception as e:
            print(f'Writing to database failed. Error message:\n{e}')

    def insert_data_to_triplets_sample(self, data_tuple_list):
        self.insert_data_tuple_to_db('triplets_sample', data_tuple_list)

    def insert_data_to_unique_tracks(self, data_tuple_list):
        self.insert_data_tuple_to_db('unique_tracks', data_tuple_list)

    def get_most_popular_tracks(self):
        self._cursor.execute(
            '''
                SELECT
                    ut.track,
                    p.playing_count
                FROM unique_tracks ut
                LEFT JOIN (
                    SELECT
                        track_id,
                        count(*) AS playing_count
                    FROM triplets_sample
                    GROUP BY track_id
                ) p
                ON ut.track_id = p.track_id
                ORDER BY playing_count DESC
                LIMIT 5
            '''
        )

        return self._cursor.fetchall()


    def get_most_popular_artist(self):
        self._cursor.execute(
            '''
                SELECT
                    artist,
                    summary_playing_count_for_artist
                FROM (
                    SELECT 
                        ut.artist,
                        sum(p.playing_count) as summary_playing_count_for_artist
                    FROM (
                        SELECT DISTINCT
                            track_id,
                            artist
                        FROM unique_tracks
                    ) ut
                    LEFT JOIN (
                        SELECT
                            track_id,
                            count(*) AS playing_count
                        FROM triplets_sample
                        GROUP BY track_id
                    ) p
                    ON ut.track_id = p.track_id
                    GROUP BY ut.artist
                )
                ORDER BY summary_playing_count_for_artist DESC
                LIMIT 1
            '''
        )

        return self._cursor.fetchall()