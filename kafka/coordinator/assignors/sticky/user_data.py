from kafka.protocol.new.api_data import ApiData


class StickyAssignorUserData(ApiData, load_json=__package__):
    def __init__(self, *args, **kw):
        if 'version' not in kw:
            kw['version'] = 1
        super().__init__(*args, **kw)
