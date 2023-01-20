class ProjectViewTable:
    def from_record(record):
        view = ProjectViewTable()
        view.name = record.name
        view.metadata = {
            'id': record.id
        }
        return view
