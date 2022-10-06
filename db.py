import datetime
import uuid
from pymongo import MongoClient
from bson.binary import Binary
import gridfs
from datetime import datetime as dt


class DatabaseConnection:
    client = None

    def __init__(self, conn_string, files_db, event_db):
        self.client = None
        self.conn_string = conn_string
        self.files_db = files_db
        self.event_db = event_db

    def connect(self):
        # if 'localhost' in self.conn_string:
        self.client = MongoClient(self.conn_string)
        # else:
        #     self.client = MongoClient(self.conn_string,
        #                               tls=True,
        #                               tlsCertificateKeyFile='config/agustin2022.pem',
        #                               server_api=ServerApi('1'))

    def close_connection(self):
        self.client.close()
        print("Se ha cerrado la conexion a la DB!")

    def connect_local(self):
        self.client = MongoClient(self.conn_string)

    def insert_video(self, filename):
        db = self.client[self.files_db]
        fs = gridfs.GridFS(db)
        with open(f'videos/{filename}', "rb") as f:
            encoded = Binary(f.read())
        file_id = fs.put(data=encoded, filename=filename)
        print(f'the file with id: {file_id} has been saved')
        ret = {
            "id": file_id,
            "filename": f"{filename}",
            "uuid": filename,
            "msg": "The file has been saved!"
        }
        return ret

    def load_from_db_grid(self, filename):
        db = self.client['tesis']
        # collection = db['files']
        fs = gridfs.GridFS(db)
        document = fs.find_one({
            "filename": filename
        })
        print(document)
        binary_data = document.read()
        with open(f'videos/{filename}', "wb") as f:
            data = Binary(binary_data)
            f.write(data)
        print(f" El archivo {document.filename} ha sido guardado")

    def load_from_db_dict(self, search_dict):
        db = self.client['tesis']
        # collection = db['files']
        fs = gridfs.GridFS(db)
        document = fs.find_one(search_dict)
        if document is None:
            return FileNotFoundError
        print(f'Se encontro un archivo con nombre: {document.filename}')
        binary_data = document.read()
        with open(f'videos/{document.filename}', "wb") as f:
            data = Binary(binary_data)
            f.write(data)
        print(f" El archivo {document.filename} ha sido guardado")
        return document

    def load_event_file(self, filename, database='files'):
        db = self.client[database]
        fs = gridfs.GridFS(db)
        document = fs.find_one({
            "filename": filename
        })
        print(document)
        binary_data = document.read()
        print(f" El archivo {document.filename} ha sido encontrado")
        return Binary(binary_data)

    # Se pasa el payload del archivo, que seria la foto
    # que vino por mqtt.
    # Devuelve el nombre del archivo asignado.
    def insert_image(self, payload):
        filename = str(uuid.uuid4())
        db = self.client[self.files_db]
        fs = gridfs.GridFS(db)
        encoded = Binary(payload)
        file_id = fs.put(data=encoded, filename=f"{filename}.jpg", uuid=filename)
        ret = {
            "id": file_id,
            "filename": f"{filename}.jpg",
            "uuid": filename,
            "msg": "The file has been saved!"
        }
        return ret

    def insert_event(self, event_collection, event_content):
        db = self.client[self.event_db]
        collection = db[event_collection]
        result = collection.insert_one(event_content)
        ret = {
            "inserted_id": result.inserted_id,
            "msg": "The record has been saved!"
        }
        return ret

    def get_device_by_ip(self, devices_collection, ip):
        db = self.client[self.event_db]
        collection = db[devices_collection]
        document = collection.find_one(
            {
                "ip_address": ip
            }
        )
        return document

    def get_user_by_rfid(self, devices_collection, rfid):
        db = self.client[self.event_db]
        collection = db[devices_collection]
        document = collection.find_one(
            {
                "code": rfid
            }
        )
        return document

    def get_timezone_by_id(self, tz_collection, tz_id):
        db = self.client[self.event_db]
        collection = db[tz_collection]
        document = collection.find_one(
            {
                "id": tz_id
            }
        )
        return document

    def get_events_duration(self, collection: str) -> dict:
        db = self.client[self.event_db]
        collection = db[collection]
        document = collection.find_one({"id": 1})
        return document

    def delete_events_duration(self, collection: str, duration: datetime) -> str:
        db = self.client[self.event_db]
        collection = db[collection]
        res = collection.delete_many(
            {
               'date_time': {'$lt': dt.isoformat(duration)}
            })
        print(f"{collection.name}: {res.raw_result}")
        return res.raw_result

    def delete_files_duration(self, duration: datetime) -> dict:
        db = self.client[self.files_db]
        fs = gridfs.GridFS(db)

        cursor = fs.find({'uploadDate': {'$lt': duration}})
        for file in cursor:
            print(f"File {file._id} being deleted!")
            fs.delete(file_id=file._id)
        ret = {
            'msg': f'deleted {cursor.retrieved} files'
            }
        print(ret)
        return ret
