BROKER_API_VERSIONS = {
    # api_versions responses prior to (0, 10) are synthesized for compatibility
    (0, 8, 0): {0: (0, 0), 1: (0, 0), 2: (0, 0), 3: (0, 0)},
    # adds offset commit + fetch
    (0, 8, 1): {0: (0, 0), 1: (0, 0), 2: (0, 0), 3: (0, 0), 8: (0, 0), 9: (0, 0)},
    # adds find coordinator
    (0, 8, 2): {0: (0, 0), 1: (0, 0), 2: (0, 0), 3: (0, 0), 8: (0, 1), 9: (0, 1), 10: (0, 0)},
    # adds group management (join/sync/leave/heartbeat)
    (0, 9): {0: (0, 1), 1: (0, 1), 2: (0, 0), 3: (0, 0), 8: (0, 2), 9: (0, 1), 10: (0, 0), 11: (0, 0), 12: (0, 0), 13: (0, 0), 14: (0, 0), 15: (0, 0), 16: (0, 0)},
    # adds message format v1, sasl, and api versions api
    (0, 10, 0): {0: (0, 2), 1: (0, 2), 2: (0, 0), 3: (0, 1), 4: (0, 0), 5: (0, 0), 6: (0, 2), 7: (1, 1), 8: (0, 2), 9: (0, 1), 10: (0, 0), 11: (0, 0), 12: (0, 0), 13: (0, 0), 14: (0, 0), 15: (0, 0), 16: (0, 0), 17: (0, 0), 18: (0, 0)},

    # All data below is copied from brokers via api_versions_response (see make servers/*/api_versions)
    # adds admin apis create/delete topics, and bumps fetch/listoffsets/metadata/joingroup
    (0, 10, 1): {0: (0, 2), 1: (0, 3), 2: (0, 1), 3: (0, 2), 4: (0, 0), 5: (0, 0), 6: (0, 2), 7: (1, 1), 8: (0, 2), 9: (0, 1), 10: (0, 0), 11: (0, 1), 12: (0, 0), 13: (0, 0), 14: (0, 0), 15: (0, 0), 16: (0, 0), 17: (0, 0), 18: (0, 0), 19: (0, 0), 20: (0, 0)},

    # bumps offsetfetch/create-topics
    (0, 10, 2): {0: (0, 2), 1: (0, 3), 2: (0, 1), 3: (0, 2), 4: (0, 0), 5: (0, 0), 6: (0, 3), 7: (1, 1), 8: (0, 2), 9: (0, 2), 10: (0, 0), 11: (0, 1), 12: (0, 0), 13: (0, 0), 14: (0, 0), 15: (0, 0), 16: (0, 0), 17: (0, 0), 18: (0, 0), 19: (0, 1), 20: (0, 0)},

    # Adds message format v2, and more admin apis (describe/create/delete acls, describe/alter configs, etc)
    (0, 11): {0: (0, 3), 1: (0, 5), 2: (0, 2), 3: (0, 4), 4: (0, 0), 5: (0, 0), 6: (0, 3), 7: (1, 1), 8: (0, 3), 9: (0, 3), 10: (0, 1), 11: (0, 2), 12: (0, 1), 13: (0, 1), 14: (0, 1), 15: (0, 1), 16: (0, 1), 17: (0, 0), 18: (0, 1), 19: (0, 2), 20: (0, 1), 21: (0, 0), 22: (0, 0), 23: (0, 0), 24: (0, 0), 25: (0, 0), 26: (0, 0), 27: (0, 0), 28: (0, 0), 29: (0, 0), 30: (0, 0), 31: (0, 0), 32: (0, 0), 33: (0, 0)},

    # Adds Sasl Authenticate, and additional admin apis (describe/alter log dirs, etc)
    (1, 0): {0: (0, 5), 1: (0, 6), 2: (0, 2), 3: (0, 5), 4: (0, 1), 5: (0, 0), 6: (0, 4), 7: (0, 1), 8: (0, 3), 9: (0, 3), 10: (0, 1), 11: (0, 2), 12: (0, 1), 13: (0, 1), 14: (0, 1), 15: (0, 1), 16: (0, 1), 17: (0, 1), 18: (0, 1), 19: (0, 2), 20: (0, 1), 21: (0, 0), 22: (0, 0), 23: (0, 0), 24: (0, 0), 25: (0, 0), 26: (0, 0), 27: (0, 0), 28: (0, 0), 29: (0, 0), 30: (0, 0), 31: (0, 0), 32: (0, 0), 33: (0, 0), 34: (0, 0), 35: (0, 0), 36: (0, 0), 37: (0, 0)},

    (2, 0): {0: (0, 6), 1: (0, 8), 2: (0, 3), 3: (0, 6), 4: (0, 1), 5: (0, 0), 6: (0, 4), 7: (0, 1), 8: (0, 4), 9: (0, 4), 10: (0, 2), 11: (0, 3), 12: (0, 2), 13: (0, 2), 14: (0, 2), 15: (0, 2), 16: (0, 2), 17: (0, 1), 18: (0, 2), 19: (0, 3), 20: (0, 2), 21: (0, 1), 22: (0, 1), 23: (0, 1), 24: (0, 1), 25: (0, 1), 26: (0, 1), 27: (0, 0), 28: (0, 1), 29: (0, 1), 30: (0, 1), 31: (0, 1), 32: (0, 2), 33: (0, 1), 34: (0, 1), 35: (0, 1), 36: (0, 0), 37: (0, 1), 38: (0, 1), 39: (0, 1), 40: (0, 1), 41: (0, 1), 42: (0, 1)},

    (2, 1): {0: (0, 7), 1: (0, 10), 2: (0, 4), 3: (0, 7), 4: (0, 1), 5: (0, 0), 6: (0, 4), 7: (0, 1), 8: (0, 6), 9: (0, 5), 10: (0, 2), 11: (0, 3), 12: (0, 2), 13: (0, 2), 14: (0, 2), 15: (0, 2), 16: (0, 2), 17: (0, 1), 18: (0, 2), 19: (0, 3), 20: (0, 3), 21: (0, 1), 22: (0, 1), 23: (0, 2), 24: (0, 1), 25: (0, 1), 26: (0, 1), 27: (0, 0), 28: (0, 2), 29: (0, 1), 30: (0, 1), 31: (0, 1), 32: (0, 2), 33: (0, 1), 34: (0, 1), 35: (0, 1), 36: (0, 0), 37: (0, 1), 38: (0, 1), 39: (0, 1), 40: (0, 1), 41: (0, 1), 42: (0, 1)},

    (2, 2): {0: (0, 7), 1: (0, 10), 2: (0, 5), 3: (0, 7), 4: (0, 2), 5: (0, 1), 6: (0, 5), 7: (0, 2), 8: (0, 6), 9: (0, 5), 10: (0, 2), 11: (0, 4), 12: (0, 2), 13: (0, 2), 14: (0, 2), 15: (0, 2), 16: (0, 2), 17: (0, 1), 18: (0, 2), 19: (0, 3), 20: (0, 3), 21: (0, 1), 22: (0, 1), 23: (0, 2), 24: (0, 1), 25: (0, 1), 26: (0, 1), 27: (0, 0), 28: (0, 2), 29: (0, 1), 30: (0, 1), 31: (0, 1), 32: (0, 2), 33: (0, 1), 34: (0, 1), 35: (0, 1), 36: (0, 1), 37: (0, 1), 38: (0, 1), 39: (0, 1), 40: (0, 1), 41: (0, 1), 42: (0, 1), 43: (0, 0)},

    (2, 3): {0: (0, 7), 1: (0, 11), 2: (0, 5), 3: (0, 8), 4: (0, 2), 5: (0, 1), 6: (0, 5), 7: (0, 2), 8: (0, 7), 9: (0, 5), 10: (0, 2), 11: (0, 5), 12: (0, 3), 13: (0, 2), 14: (0, 3), 15: (0, 3), 16: (0, 2), 17: (0, 1), 18: (0, 2), 19: (0, 3), 20: (0, 3), 21: (0, 1), 22: (0, 1), 23: (0, 3), 24: (0, 1), 25: (0, 1), 26: (0, 1), 27: (0, 0), 28: (0, 2), 29: (0, 1), 30: (0, 1), 31: (0, 1), 32: (0, 2), 33: (0, 1), 34: (0, 1), 35: (0, 1), 36: (0, 1), 37: (0, 1), 38: (0, 1), 39: (0, 1), 40: (0, 1), 41: (0, 1), 42: (0, 1), 43: (0, 0), 44: (0, 0)},

    (2, 4): {0: (0, 8), 1: (0, 11), 2: (0, 5), 3: (0, 9), 4: (0, 4), 5: (0, 2), 6: (0, 6), 7: (0, 3), 8: (0, 8), 9: (0, 6), 10: (0, 3), 11: (0, 6), 12: (0, 4), 13: (0, 4), 14: (0, 4), 15: (0, 5), 16: (0, 3), 17: (0, 1), 18: (0, 3), 19: (0, 5), 20: (0, 4), 21: (0, 1), 22: (0, 2), 23: (0, 3), 24: (0, 1), 25: (0, 1), 26: (0, 1), 27: (0, 0), 28: (0, 2), 29: (0, 1), 30: (0, 1), 31: (0, 1), 32: (0, 2), 33: (0, 1), 34: (0, 1), 35: (0, 1), 36: (0, 1), 37: (0, 1), 38: (0, 2), 39: (0, 1), 40: (0, 1), 41: (0, 1), 42: (0, 2), 43: (0, 2), 44: (0, 1), 45: (0, 0), 46: (0, 0), 47: (0, 0)},

    (2, 5): {0: (0, 8), 1: (0, 11), 2: (0, 5), 3: (0, 9), 4: (0, 4), 5: (0, 2), 6: (0, 6), 7: (0, 3), 8: (0, 8), 9: (0, 7), 10: (0, 3), 11: (0, 7), 12: (0, 4), 13: (0, 4), 14: (0, 5), 15: (0, 5), 16: (0, 3), 17: (0, 1), 18: (0, 3), 19: (0, 5), 20: (0, 4), 21: (0, 1), 22: (0, 3), 23: (0, 3), 24: (0, 1), 25: (0, 1), 26: (0, 1), 27: (0, 0), 28: (0, 3), 29: (0, 2), 30: (0, 2), 31: (0, 2), 32: (0, 2), 33: (0, 1), 34: (0, 1), 35: (0, 1), 36: (0, 2), 37: (0, 2), 38: (0, 2), 39: (0, 2), 40: (0, 2), 41: (0, 2), 42: (0, 2), 43: (0, 2), 44: (0, 1), 45: (0, 0), 46: (0, 0), 47: (0, 0)},

    (2, 6): {0: (0, 8), 1: (0, 11), 2: (0, 5), 3: (0, 9), 4: (0, 4), 5: (0, 3), 6: (0, 6), 7: (0, 3), 8: (0, 8), 9: (0, 7), 10: (0, 3), 11: (0, 7), 12: (0, 4), 13: (0, 4), 14: (0, 5), 15: (0, 5), 16: (0, 4), 17: (0, 1), 18: (0, 3), 19: (0, 5), 20: (0, 4), 21: (0, 2), 22: (0, 3), 23: (0, 3), 24: (0, 1), 25: (0, 1), 26: (0, 1), 27: (0, 0), 28: (0, 3), 29: (0, 2), 30: (0, 2), 31: (0, 2), 32: (0, 3), 33: (0, 1), 34: (0, 1), 35: (0, 2), 36: (0, 2), 37: (0, 2), 38: (0, 2), 39: (0, 2), 40: (0, 2), 41: (0, 2), 42: (0, 2), 43: (0, 2), 44: (0, 1), 45: (0, 0), 46: (0, 0), 47: (0, 0), 48: (0, 0), 49: (0, 0)},

    (2, 7): {0: (0, 8), 1: (0, 12), 2: (0, 5), 3: (0, 9), 4: (0, 4), 5: (0, 3), 6: (0, 6), 7: (0, 3), 8: (0, 8), 9: (0, 7), 10: (0, 3), 11: (0, 7), 12: (0, 4), 13: (0, 4), 14: (0, 5), 15: (0, 5), 16: (0, 4), 17: (0, 1), 18: (0, 3), 19: (0, 6), 20: (0, 5), 21: (0, 2), 22: (0, 4), 23: (0, 3), 24: (0, 2), 25: (0, 2), 26: (0, 2), 27: (0, 0), 28: (0, 3), 29: (0, 2), 30: (0, 2), 31: (0, 2), 32: (0, 3), 33: (0, 1), 34: (0, 1), 35: (0, 2), 36: (0, 2), 37: (0, 3), 38: (0, 2), 39: (0, 2), 40: (0, 2), 41: (0, 2), 42: (0, 2), 43: (0, 2), 44: (0, 1), 45: (0, 0), 46: (0, 0), 47: (0, 0), 48: (0, 0), 49: (0, 0), 50: (0, 0), 51: (0, 0), 56: (0, 0), 57: (0, 0)},

    (2, 8): {0: (0, 9), 1: (0, 12), 2: (0, 6), 3: (0, 11), 4: (0, 5), 5: (0, 3), 6: (0, 7), 7: (0, 3), 8: (0, 8), 9: (0, 7), 10: (0, 3), 11: (0, 7), 12: (0, 4), 13: (0, 4), 14: (0, 5), 15: (0, 5), 16: (0, 4), 17: (0, 1), 18: (0, 3), 19: (0, 7), 20: (0, 6), 21: (0, 2), 22: (0, 4), 23: (0, 4), 24: (0, 3), 25: (0, 3), 26: (0, 3), 27: (0, 1), 28: (0, 3), 29: (0, 2), 30: (0, 2), 31: (0, 2), 32: (0, 4), 33: (0, 2), 34: (0, 2), 35: (0, 2), 36: (0, 2), 37: (0, 3), 38: (0, 2), 39: (0, 2), 40: (0, 2), 41: (0, 2), 42: (0, 2), 43: (0, 2), 44: (0, 1), 45: (0, 0), 46: (0, 0), 47: (0, 0), 48: (0, 1), 49: (0, 1), 50: (0, 0), 51: (0, 0), 56: (0, 0), 57: (0, 0), 60: (0, 0), 61: (0, 0)},

    (3, 0): {0: (0, 9), 1: (0, 12), 2: (0, 7), 3: (0, 11), 4: (0, 5), 5: (0, 3), 6: (0, 7), 7: (0, 3), 8: (0, 8), 9: (0, 8), 10: (0, 4), 11: (0, 7), 12: (0, 4), 13: (0, 4), 14: (0, 5), 15: (0, 5), 16: (0, 4), 17: (0, 1), 18: (0, 3), 19: (0, 7), 20: (0, 6), 21: (0, 2), 22: (0, 4), 23: (0, 4), 24: (0, 3), 25: (0, 3), 26: (0, 3), 27: (0, 1), 28: (0, 3), 29: (0, 2), 30: (0, 2), 31: (0, 2), 32: (0, 4), 33: (0, 2), 34: (0, 2), 35: (0, 2), 36: (0, 2), 37: (0, 3), 38: (0, 2), 39: (0, 2), 40: (0, 2), 41: (0, 2), 42: (0, 2), 43: (0, 2), 44: (0, 1), 45: (0, 0), 46: (0, 0), 47: (0, 0), 48: (0, 1), 49: (0, 1), 50: (0, 0), 51: (0, 0), 56: (0, 0), 57: (0, 0), 60: (0, 0), 61: (0, 0), 65: (0, 0), 66: (0, 0), 67: (0, 0)},

    (3, 1): {0: (0, 9), 1: (0, 13), 2: (0, 7), 3: (0, 12), 4: (0, 5), 5: (0, 3), 6: (0, 7), 7: (0, 3), 8: (0, 8), 9: (0, 8), 10: (0, 4), 11: (0, 7), 12: (0, 4), 13: (0, 4), 14: (0, 5), 15: (0, 5), 16: (0, 4), 17: (0, 1), 18: (0, 3), 19: (0, 7), 20: (0, 6), 21: (0, 2), 22: (0, 4), 23: (0, 4), 24: (0, 3), 25: (0, 3), 26: (0, 3), 27: (0, 1), 28: (0, 3), 29: (0, 2), 30: (0, 2), 31: (0, 2), 32: (0, 4), 33: (0, 2), 34: (0, 2), 35: (0, 2), 36: (0, 2), 37: (0, 3), 38: (0, 2), 39: (0, 2), 40: (0, 2), 41: (0, 2), 42: (0, 2), 43: (0, 2), 44: (0, 1), 45: (0, 0), 46: (0, 0), 47: (0, 0), 48: (0, 1), 49: (0, 1), 50: (0, 0), 51: (0, 0), 56: (0, 0), 57: (0, 0), 60: (0, 0), 61: (0, 0), 65: (0, 0), 66: (0, 0), 67: (0, 0)},

    (3, 2): {0: (0, 9), 1: (0, 13), 2: (0, 7), 3: (0, 12), 4: (0, 6), 5: (0, 3), 6: (0, 7), 7: (0, 3), 8: (0, 8), 9: (0, 8), 10: (0, 4), 11: (0, 9), 12: (0, 4), 13: (0, 5), 14: (0, 5), 15: (0, 5), 16: (0, 4), 17: (0, 1), 18: (0, 3), 19: (0, 7), 20: (0, 6), 21: (0, 2), 22: (0, 4), 23: (0, 4), 24: (0, 3), 25: (0, 3), 26: (0, 3), 27: (0, 1), 28: (0, 3), 29: (0, 2), 30: (0, 2), 31: (0, 2), 32: (0, 4), 33: (0, 2), 34: (0, 2), 35: (0, 3), 36: (0, 2), 37: (0, 3), 38: (0, 2), 39: (0, 2), 40: (0, 2), 41: (0, 2), 42: (0, 2), 43: (0, 2), 44: (0, 1), 45: (0, 0), 46: (0, 0), 47: (0, 0), 48: (0, 1), 49: (0, 1), 50: (0, 0), 51: (0, 0), 56: (0, 1), 57: (0, 0), 60: (0, 0), 61: (0, 0), 65: (0, 0), 66: (0, 0), 67: (0, 0)},

    (3, 3): {0: (0, 9), 1: (0, 13), 2: (0, 7), 3: (0, 12), 4: (0, 6), 5: (0, 3), 6: (0, 7), 7: (0, 3), 8: (0, 8), 9: (0, 8), 10: (0, 4), 11: (0, 9), 12: (0, 4), 13: (0, 5), 14: (0, 5), 15: (0, 5), 16: (0, 4), 17: (0, 1), 18: (0, 3), 19: (0, 7), 20: (0, 6), 21: (0, 2), 22: (0, 4), 23: (0, 4), 24: (0, 3), 25: (0, 3), 26: (0, 3), 27: (0, 1), 28: (0, 3), 29: (0, 3), 30: (0, 3), 31: (0, 3), 32: (0, 4), 33: (0, 2), 34: (0, 2), 35: (0, 4), 36: (0, 2), 37: (0, 3), 38: (0, 3), 39: (0, 2), 40: (0, 2), 41: (0, 3), 42: (0, 2), 43: (0, 2), 44: (0, 1), 45: (0, 0), 46: (0, 0), 47: (0, 0), 48: (0, 1), 49: (0, 1), 50: (0, 0), 51: (0, 0), 56: (0, 2), 57: (0, 1), 60: (0, 0), 61: (0, 0), 65: (0, 0), 66: (0, 0), 67: (0, 0)},

    (3, 4): {0: (0, 9), 1: (0, 13), 2: (0, 7), 3: (0, 12), 4: (0, 7), 5: (0, 4), 6: (0, 8), 7: (0, 3), 8: (0, 8), 9: (0, 8), 10: (0, 4), 11: (0, 9), 12: (0, 4), 13: (0, 5), 14: (0, 5), 15: (0, 5), 16: (0, 4), 17: (0, 1), 18: (0, 3), 19: (0, 7), 20: (0, 6), 21: (0, 2), 22: (0, 4), 23: (0, 4), 24: (0, 3), 25: (0, 3), 26: (0, 3), 27: (0, 1), 28: (0, 3), 29: (0, 3), 30: (0, 3), 31: (0, 3), 32: (0, 4), 33: (0, 2), 34: (0, 2), 35: (0, 4), 36: (0, 2), 37: (0, 3), 38: (0, 3), 39: (0, 2), 40: (0, 2), 41: (0, 3), 42: (0, 2), 43: (0, 2), 44: (0, 1), 45: (0, 0), 46: (0, 0), 47: (0, 0), 48: (0, 1), 49: (0, 1), 50: (0, 0), 51: (0, 0), 56: (0, 2), 57: (0, 1), 58: (0, 0), 60: (0, 0), 61: (0, 0), 65: (0, 0), 66: (0, 0), 67: (0, 0)},

    (3, 5): {0: (0, 9), 1: (0, 15), 2: (0, 8), 3: (0, 12), 4: (0, 7), 5: (0, 4), 6: (0, 8), 7: (0, 3), 8: (0, 8), 9: (0, 8), 10: (0, 4), 11: (0, 9), 12: (0, 4), 13: (0, 5), 14: (0, 5), 15: (0, 5), 16: (0, 4), 17: (0, 1), 18: (0, 3), 19: (0, 7), 20: (0, 6), 21: (0, 2), 22: (0, 4), 23: (0, 4), 24: (0, 3), 25: (0, 3), 26: (0, 3), 27: (0, 1), 28: (0, 3), 29: (0, 3), 30: (0, 3), 31: (0, 3), 32: (0, 4), 33: (0, 2), 34: (0, 2), 35: (0, 4), 36: (0, 2), 37: (0, 3), 38: (0, 3), 39: (0, 2), 40: (0, 2), 41: (0, 3), 42: (0, 2), 43: (0, 2), 44: (0, 1), 45: (0, 0), 46: (0, 0), 47: (0, 0), 48: (0, 1), 49: (0, 1), 50: (0, 0), 51: (0, 0), 56: (0, 3), 57: (0, 1), 58: (0, 0), 60: (0, 0), 61: (0, 0), 65: (0, 0), 66: (0, 0), 67: (0, 0)},

    (3, 6): {0: (0, 9), 1: (0, 15), 2: (0, 8), 3: (0, 12), 4: (0, 7), 5: (0, 4), 6: (0, 8), 7: (0, 3), 8: (0, 8), 9: (0, 8), 10: (0, 4), 11: (0, 9), 12: (0, 4), 13: (0, 5), 14: (0, 5), 15: (0, 5), 16: (0, 4), 17: (0, 1), 18: (0, 3), 19: (0, 7), 20: (0, 6), 21: (0, 2), 22: (0, 4), 23: (0, 4), 24: (0, 4), 25: (0, 3), 26: (0, 3), 27: (0, 1), 28: (0, 3), 29: (0, 3), 30: (0, 3), 31: (0, 3), 32: (0, 4), 33: (0, 2), 34: (0, 2), 35: (0, 4), 36: (0, 2), 37: (0, 3), 38: (0, 3), 39: (0, 2), 40: (0, 2), 41: (0, 3), 42: (0, 2), 43: (0, 2), 44: (0, 1), 45: (0, 0), 46: (0, 0), 47: (0, 0), 48: (0, 1), 49: (0, 1), 50: (0, 0), 51: (0, 0), 56: (0, 3), 57: (0, 1), 58: (0, 0), 60: (0, 0), 61: (0, 0), 65: (0, 0), 66: (0, 0), 67: (0, 0)},

    (3, 7): {0: (0, 10), 1: (0, 16), 2: (0, 8), 3: (0, 12), 4: (0, 7), 5: (0, 4), 6: (0, 8), 7: (0, 3), 8: (0, 9), 9: (0, 9), 10: (0, 4), 11: (0, 9), 12: (0, 4), 13: (0, 5), 14: (0, 5), 15: (0, 5), 16: (0, 4), 17: (0, 1), 18: (0, 3), 19: (0, 7), 20: (0, 6), 21: (0, 2), 22: (0, 4), 23: (0, 4), 24: (0, 4), 25: (0, 3), 26: (0, 3), 27: (0, 1), 28: (0, 3), 29: (0, 3), 30: (0, 3), 31: (0, 3), 32: (0, 4), 33: (0, 2), 34: (0, 2), 35: (0, 4), 36: (0, 2), 37: (0, 3), 38: (0, 3), 39: (0, 2), 40: (0, 2), 41: (0, 3), 42: (0, 2), 43: (0, 2), 44: (0, 1), 45: (0, 0), 46: (0, 0), 47: (0, 0), 48: (0, 1), 49: (0, 1), 50: (0, 0), 51: (0, 0), 56: (0, 3), 57: (0, 1), 58: (0, 0), 60: (0, 1), 61: (0, 0), 65: (0, 0), 66: (0, 0), 67: (0, 0), 68: (0, 0)},

    (3, 8): {0: (0, 11), 1: (0, 16), 2: (0, 8), 3: (0, 12), 4: (0, 7), 5: (0, 4), 6: (0, 8), 7: (0, 3), 8: (0, 9), 9: (0, 9), 10: (0, 5), 11: (0, 9), 12: (0, 4), 13: (0, 5), 14: (0, 5), 15: (0, 5), 16: (0, 5), 17: (0, 1), 18: (0, 3), 19: (0, 7), 20: (0, 6), 21: (0, 2), 22: (0, 5), 23: (0, 4), 24: (0, 5), 25: (0, 4), 26: (0, 4), 27: (0, 1), 28: (0, 4), 29: (0, 3), 30: (0, 3), 31: (0, 3), 32: (0, 4), 33: (0, 2), 34: (0, 2), 35: (0, 4), 36: (0, 2), 37: (0, 3), 38: (0, 3), 39: (0, 2), 40: (0, 2), 41: (0, 3), 42: (0, 2), 43: (0, 2), 44: (0, 1), 45: (0, 0), 46: (0, 0), 47: (0, 0), 48: (0, 1), 49: (0, 1), 50: (0, 0), 51: (0, 0), 56: (0, 3), 57: (0, 1), 58: (0, 0), 60: (0, 1), 61: (0, 0), 65: (0, 0), 66: (0, 1), 67: (0, 0), 68: (0, 0), 69: (0, 0)},

    (3, 9): {0: (0, 11), 1: (0, 17), 2: (0, 9), 3: (0, 12), 4: (0, 7), 5: (0, 4), 6: (0, 8), 7: (0, 3), 8: (0, 9), 9: (0, 9), 10: (0, 6), 11: (0, 9), 12: (0, 4), 13: (0, 5), 14: (0, 5), 15: (0, 5), 16: (0, 5), 17: (0, 1), 18: (0, 4), 19: (0, 7), 20: (0, 6), 21: (0, 2), 22: (0, 5), 23: (0, 4), 24: (0, 5), 25: (0, 4), 26: (0, 4), 27: (0, 1), 28: (0, 4), 29: (0, 3), 30: (0, 3), 31: (0, 3), 32: (0, 4), 33: (0, 2), 34: (0, 2), 35: (0, 4), 36: (0, 2), 37: (0, 3), 38: (0, 3), 39: (0, 2), 40: (0, 2), 41: (0, 3), 42: (0, 2), 43: (0, 2), 44: (0, 1), 45: (0, 0), 46: (0, 0), 47: (0, 0), 48: (0, 1), 49: (0, 1), 50: (0, 0), 51: (0, 0), 56: (0, 3), 57: (0, 1), 58: (0, 0), 60: (0, 1), 61: (0, 0), 65: (0, 0), 66: (0, 1), 67: (0, 0), 68: (0, 0), 69: (0, 0)},

}
