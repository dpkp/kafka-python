import os

if os.environ.get('USE_GEVENT') == '1':
    import gevent.monkey
    gevent.monkey.patch_all(Event=True)
