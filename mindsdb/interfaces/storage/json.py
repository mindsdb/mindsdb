from mindsdb.interfaces.storage import db
from mindsdb.interfaces.storage.fs import RESOURCE_GROUP
from mindsdb.utilities.context import context as ctx


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
            db.session.delete(record)

    def delete(self, key):
        self.delete(key)


def get_json_storage(resource_id: int, resource_group: str = RESOURCE_GROUP.PREDICTOR):
    return JsonStorage(
        resource_group=resource_group,
        resource_id=resource_id,
    )
