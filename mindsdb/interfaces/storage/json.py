from typing import Optional

from mindsdb.interfaces.storage.db import session, JsonStorage as JsonStorageTable


class JsonStorage:
    def __init__(self, resource_group: str, resource_id: int, company_id: Optional[int] = None):
        self.resource_group = resource_group
        self.resource_id = resource_id
        self.company_id = company_id

    def __setitem__(self, key, value):
        if isinstance(value, dict) is False:
            raise TypeError(f"got {type(value)} instead of dict")
        existing_record = self.get_record(key)
        if existing_record is None:
            record = JsonStorageTable(
                name=key,
                resource_group=self.resource_group,
                resource_id=self.resource_id,
                company_id=self.company_id,
                content=value
            )
            session.add(record)
        else:
            existing_record.content = value
        session.commit()

    def __getitem__(self, key):
        record = self.get_record(key)
        return record.content

    def get_record(self, key):
        record = session.query(JsonStorageTable).filter_by(
            name=key,
            resource_group=self.resource_group,
            resource_id=self.resource_id,
            company_id=self.company_id
        ).first()
        return record

    def get_all_records(self):
        records = session.query(JsonStorageTable).filter_by(
            resource_group=self.resource_group,
            resource_id=self.resource_id,
            company_id=self.company_id
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
            session.delete(record)
