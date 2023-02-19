import hashlib
import random
import string
import uuid


def get_rnd_digits(k: int) -> str:
    return ''.join(random.choices(string.digits, k=k))


def get_uuid() -> str:
    return str(uuid.uuid4())


def get_system_id() -> int:
    return random.randint(1, 1000000)


def get_inn(event_type: str) -> tuple:
    inn = get_rnd_digits(12)
    if event_type == 'SUBMIT_MD5':
        data_value = hashlib.md5(inn.encode()).hexdigest()
    elif event_type == 'SUBMIT':
        data_value = hashlib.sha256(inn.encode()).hexdigest()
    else:
        data_value, inn = None, None
    return data_value, inn
