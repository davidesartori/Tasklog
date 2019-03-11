import firebase_admin
from firebase_admin import credentials
from firebase_admin import db


def connection():
    cred = credentials.Certificate('admin-sdk.json')

    firebase_admin.initialize_app(cred, {
        'databaseURL': 'https://processi-marconi.firebaseio.com/'
    })


def update(ref_name, db_structure):
    ref = db.reference(ref_name)

    ref.update(
        db_structure
    )


if __name__ == "__main__":
    connection()

    db_ref = "orario"
    structure = {'event': 'caricamento completato'}

    update(db_ref, structure)
