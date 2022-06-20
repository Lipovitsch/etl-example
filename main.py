from sqlite_connector import SQLiteConnector
from time import time


def print_most_popular_artist(db_connector):
    artist = db_connector.get_most_popular_artist()
    print('\nMost popular artist:')
    print(f'{artist[0][0]}: {artist[0][1]} plays')


def print_most_popular_tracks(db_connector):
    most_popular_tracks = db_connector.get_most_popular_tracks()
    print('\nMost popular tracks:')
    for track in most_popular_tracks:
        print(f'{track[0]}: {track[1]} plays')


def load_triplets_sample_to_db(db_connector, insert_batch_size):
    tracks_summary = dict()

    with open('triplets_sample_20p.txt', 'r') as f:
        counter = 0
        data_tuple_list = []

        for line in f:
            data_tuple = line.replace('\n', '').split("<SEP>")

            if len(data_tuple) == 3:
                data_tuple_list.append(data_tuple)
                counter+=1

            if(counter == insert_batch_size):    
                db_connector.insert_data_to_triplets_sample(data_tuple_list)
                data_tuple_list = []
                counter = 0
        
        if(len(data_tuple_list) > 0):
            db_connector.insert_data_to_triplets_sample(data_tuple_list)


def load_unique_tracks_to_db(db_connector, insert_batch_size):
    tracks_summary = dict()

    with open('unique_tracks.txt', 'r', encoding='iso-8859-1') as f:
        counter = 0
        data_tuple_list = []

        for line in f:
            data_tuple = line.replace('\n', '').split("<SEP>")
            
            if len(data_tuple) == 4:
                data_tuple_list.append(data_tuple)
                counter+=1

            if(counter == insert_batch_size):    
                db_connector.insert_data_to_unique_tracks(data_tuple_list)
                data_tuple_list = []
                counter = 0
        
        if(len(data_tuple_list) > 0):
            db_connector.insert_data_to_unique_tracks(data_tuple_list)


def main():
    insert_batch_size = 100000
    db_connector = SQLiteConnector()
    processing_start_time = time()
    print("\nProcessing data...")

    load_triplets_sample_to_db(db_connector, insert_batch_size)
    load_unique_tracks_to_db(db_connector, insert_batch_size)
    
    print_most_popular_tracks(db_connector)
    print_most_popular_artist(db_connector)

    print(f'\nProcessing time: {round(time() - processing_start_time)} seconds\n')


if __name__ == '__main__':
    main()
