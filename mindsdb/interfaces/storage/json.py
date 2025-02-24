from mindsdb.utilities.functions import decrypt_json, encrypt_json
from mindsdb.utilities.config import config
from mindsdb.interfaces.storage import db
from mindsdb.interfaces.storage.fs import RESOURCE_GROUP
from mindsdb.utilities.context import context as ctx
from mindsdb.utilities import log

logger = log.getLogger(__name__)


class JsonStorage:
    def __init__(self, resource_group: str, resource_id: int):
        self.resource_group = resource_group
        self.resource_id = resource_id

    def __setitem__(self, key, value):
        if isinstance(value, dict) is False:
            raise TypeError(f"got {type(value)} instead of dict")
        existing_record = self.get_record(key)
        if existing_record is None:
            record = db.JsonStorage(
                name=key,
                resource_group=self.resource_group,
                resource_id=self.resource_id,
                company_id=ctx.company_id,
                content=value
            )
            db.session.add(record)
        else:
            existing_record.content = value
        db.session.commit()

    def set(self, key, value):
        self[key] = value

    def __getitem__(self, key):
        record = self.get_record(key)
        if record is None:
            return None
        return record.content

    def get(self, key):
        return self[key]

    def get_record(self, key):
        record = db.session.query(db.JsonStorage).filter_by(
            name=key,
            resource_group=self.resource_group,
            resource_id=self.resource_id,
            company_id=ctx.company_id
        ).first()
        return record

    def get_all_records(self):
        records = db.session.query(db.JsonStorage).filter_by(
            resource_group=self.resource_group,
            resource_id=self.resource_id,
            company_id=ctx.company_id
        ).all()
        return records

    def __repr__(self):
        records = self.get_all_records()
        names = [x.name for x in records]
        return f'json_storage({names})'

    def __len__(self):
        records = self.get_all_records()
        return len(records)

    def __delitem__(self, key):
        record = self.get_record(key)
        if record is not None:
            try:
                db.session.delete(record)
                db.session.commit()
            except Exception:
                db.session.rollback()
                logger.error('cant delete record from JSON storage')

    def delete(self, key):
        del self[key]

    def clean(self):
        json_records = self.get_all_records()
        for record in json_records:
            db.session.delete(record)
        try:
            db.session.commit()
        except Exception:
            db.session.rollback()
            logger.error('cant delete records from JSON storage')


class EncryptedJsonStorage(JsonStorage):
    def __init__(self, resource_group: str, resource_id: int):
        super().__init__(resource_group, resource_id)
        self.secret_key = config.get('secret_key', 'dummy-key')

    def __setitem__(self, key: str, value: dict) -> None:
        if isinstance(value, dict) is False:
            raise TypeError(f"got {type(value)} instead of dict")

        encrypted_value = encrypt_json(value, self.secret_key)

        existing_record = self.get_record(key)
        if existing_record is None:
            record = db.JsonStorage(
                name=key,
                resource_group=self.resource_group,
                resource_id=self.resource_id,
                company_id=ctx.company_id,
                encrypted_content=encrypted_value
            )
            db.session.add(record)
        else:
            existing_record.encrypted_content = encrypted_value
        db.session.commit()

    def set_bytes(self, key: str, encrypted_value: bytes):
        existing_record = self.get_record(key)
        if existing_record is None:
            record = db.JsonStorage(
                name=key,
                resource_group=self.resource_group,
                resource_id=self.resource_id,
                company_id=ctx.company_id,
                encrypted_content=encrypted_value
            )
            db.session.add(record)
        else:
            existing_record.encrypted_content = encrypted_value
        db.session.commit()

    def set_str(self, key: str, encrypted_value: str):
        self.set_bytes(key, encrypted_value.encode())

    def __getitem__(self, key: str) -> dict:
        record = self.get_record(key)
        if record is None:
            return None
        return decrypt_json(record.encrypted_content, self.secret_key)


def get_json_storage(resource_id: int, resource_group: str = RESOURCE_GROUP.PREDICTOR):
    return JsonStorage(
        resource_group=resource_group,
        resource_id=resource_id,
    )


def get_encrypted_json_storage(resource_id: int, resource_group: str = RESOURCE_GROUP.PREDICTOR):
    return EncryptedJsonStorage(
        resource_group=resource_group,
        resource_id=resource_id,
    )
