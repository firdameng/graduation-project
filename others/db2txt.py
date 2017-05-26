from pymongo import MongoClient

def main():

    database_name = 'douban_movie_comments'
    collection_name = 'movie_1291560'
    buffer_size = 20
    buffer = ''

    client = MongoClient()
    try:
        db = client.get_database(database_name)
        collection = db.get_collection(collection_name)
    except:
        print 'database access error'
    else:
        print db.name, collection.name
        cursor = collection.find()
        count = 0
        for doc in cursor:
            line = str(doc['comment-roting']) + '\t' + doc['comment-text'].strip(' ')
            buffer += line.encode('utf-8')
            count += 1
            print count
            if count == buffer_size:
                with open('{0}.txt'.format(collection_name),'w') as outfile:
                    outfile.writelines(buffer)
                count = 0
    finally:
        client.close()


if __name__ == '__main__':

    main()