import hashlib
import logging
import random
import string
import time
from datetime import date, datetime

from countryinfo import CountryInfo
import pycountry

from pyspark.sql import SparkSession, DataFrame
import pyspark.sql.functions as F
import pyspark.sql.types as t
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number

from utils import get_rnd_digits, get_uuid, get_system_id, get_inn

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s - %(filename)s:%(lineno)s:%(funcName)s()',
)
logger = logging.getLogger(__name__)


def create_df(rows: list, schema: list) -> DataFrame:
    rdd = spark.sparkContext.parallelize(rows)
    df = spark.createDataFrame(rdd, schema=schema)
    return df


def get_nested_field(key: str, struct: str = 'raw_cookie') -> F.udf:
    return F.udf(lambda array: [row for row in array if row['key'] == key][0]['value'], t.StringType())(struct)


def generate_user_data(year: int, month: int, day: int) -> tuple:
    # raw_cookie
    seconds = int(time.mktime(datetime.now().timetuple()))
    digits = get_rnd_digits(9)
    _sa_cookie_a = '.'.join(['SA1', get_uuid(), str(seconds)])
    _fa_cookie_b = '.'.join(['ads2', digits, str(seconds)])
    _ym_cookie_c = ''.join([str(seconds * 1000), digits])
    _fbp = '.'.join(['fb.1', str(seconds * 1000), digits])
    org_uid = int(get_rnd_digits(6))
    user_uid = get_uuid()

    # events
    event_type = random.choice(['SUBMIT', 'REGISTER', 'SUBMIT_MD5'])
    event_action = random.choice(['pageview', 'event', 'login-check-otp'])

    # inn
    data_value, inn = get_inn(event_type)

    # geo
    countries_list = list(pycountry.countries)
    geo_country, city = None, None
    while city is None:
        geo_country = random.choice(countries_list).name
        try:
            city = CountryInfo(geo_country).capital()
        except KeyError:
            continue

    try:
        geo_latitude = ', '.join(map(str, CountryInfo(geo_country).capital_latlng()))
    except KeyError:
        geo_latitude = ', '.join(map(str, CountryInfo('Russia').capital_latlng()))

    # user
    user_os = random.choice(['Windows', 'Linux', 'Mac'])
    system_language = random.choice(['RU', 'ENG'])
    meta_platform = random.choice(['WEB', 'MOBILE'])
    screensize = '1920x1080'
    timestamp = date(year, month, day)

    # contacts
    user_phone = ' '.join(['9', get_rnd_digits(3), get_rnd_digits(3), get_rnd_digits(2), get_rnd_digits(2)])
    hash_phone_md5 = hashlib.md5(user_phone.encode()).hexdigest()
    user_mail = ''.join([''.join(random.choices(string.ascii_letters, k=10)), '@gmail.com'])
    hash_email_md5 = hashlib.md5(user_mail.encode()).hexdigest()

    match_code = random.choice(['GAID', 'IDFA'])

    if match_code == 'IDFA':
        user_uid = user_uid.upper()

    raw_cookie = [
        {'key': '_sa_cookie_a', 'value': _sa_cookie_a},
        {'key': '_fa_cookie_b', 'value': _fa_cookie_b},
        {'key': '_ym_cookie_c', 'value': _ym_cookie_c},
        {'key': '_fbp', 'value': _fbp},
        {'key': 'org_uid', 'value': org_uid},
        {'key': 'user_uid', 'value': user_uid},
        {'key': 'user_phone', 'value': user_phone},
        {'key': 'user_mail', 'value': user_mail}
    ]

    # mart ids
    if inn is not None:
        id_b, id_d = get_system_id(), get_system_id()
    else:
        id_b, id_d = None, None

    id_c, id_e, id_f = get_system_id(), get_system_id(), get_system_id()

    logger.info(f'User {user_uid} created')

    return (raw_cookie, event_type, event_action, data_value, geo_country, city, user_os, system_language,
            geo_latitude, meta_platform, screensize, timestamp, id_b, inn, id_c, id_d, id_e, hash_phone_md5,
            id_f, hash_email_md5, match_code)


def generate_data(users: int = 100, year: int = 2023, month: int = 1, day: int = 1) -> DataFrame:
    data = []

    for i in range(users):
        data.append(generate_user_data(year, month, day))

    df = create_df(data, ['raw_cookie', 'event_type', 'event_action', 'data_value', 'geocountry', 'city', 'user_os',
                          'systemlanguage', 'geolatitude', 'meta_platform', 'screensize', 'timestampcolumn', 'id_b',
                          'inn', 'id_c', 'id_d', 'id_e', 'hash_phone_md5', 'id_f', 'hash_email_md5', 'match_code'])

    logger.info('Data generated')
    return df


def create_marts(data: DataFrame) -> tuple:
    mart_b = (
        data
        .select(F.col('id_b').alias('id'), 'inn')
        .na
        .drop('all')
    )
    mart_c = (
        data
        .select(
            F.col('id_c').alias('id'),
            get_nested_field('_sa_cookie_a').alias('cookie_a')
        )
    )
    mart_d = (
        data
        .select(F.col('id_d').alias('id'), 'inn')
        .na
        .drop('all')
    )
    mart_e = data.select(F.col('id_e').alias('id'), 'hash_phone_md5')
    mart_f = data.select(F.col('id_f').alias('id'), 'hash_email_md5')
    mart_g = (
        data
        .select(
            'match_code',
            get_nested_field('user_uid').alias('user_uid'),
            F.col('id_f').alias('id')
        )
    )
    mart_a = data.drop('id_b', 'inn', 'id_c', 'id_d', 'id_e', 'hash_phone_md5', 'id_f', 'hash_email_md5', 'match_code')

    logger.info('Source marts created')
    return mart_a, mart_b, mart_c, mart_d, mart_e, mart_f, mart_g


def get_result_mart(mart_a: DataFrame,
                    mart_b: DataFrame,
                    mart_c: DataFrame,
                    mart_d: DataFrame,
                    mart_e: DataFrame,
                    mart_f: DataFrame,
                    mart_g: DataFrame) -> DataFrame:
    logger.info('Joining marts...')
    result = (
        mart_a
        .join(
            mart_b,
            (mart_a.data_value == F.sha2(mart_b.inn, 256)) | (mart_a.data_value == F.md5(mart_b.inn)),
            'left'
        )
        .withColumnRenamed('id', 'id_b')
        .join(mart_d, ['inn'], 'left')
        .withColumnRenamed('id', 'id_d')
        .join(mart_c, get_nested_field('_sa_cookie_a') == mart_c.cookie_a, 'left')
        .withColumnRenamed('id', 'id_c')
        .join(mart_e, F.md5(get_nested_field('user_phone')) == mart_e.hash_phone_md5, 'left')
        .withColumnRenamed('id', 'id_e')
        .join(mart_f, F.md5(get_nested_field('user_mail')) == mart_f.hash_email_md5, 'left')
        .withColumnRenamed('id', 'id_f')
        .join(mart_g, get_nested_field('user_uid') == mart_g.user_uid, 'left')
        .select(
            'user_uid',
            'inn',
            get_nested_field('user_phone').alias('user_phone'),
            get_nested_field('user_mail').alias('user_mail'),
            F.col('data_value').alias('inn_hash'),
            'hash_phone_md5',
            'hash_email_md5',
            get_nested_field('org_uid').alias('org_uid'),
            'id_b', 'id_c', 'id_d', 'id_e', 'id_f',
            F.slice('raw_cookie', 1, 4).alias("cookie")
        )
    )
    logger.info(f'Resulted mart created - {result.count()} rows')
    return result


def combine_days(m_prev: DataFrame, m_cur: DataFrame) -> DataFrame:
    logger.info('Adding new users to the mart...')
    # leave the latest row if duplicated
    w = Window.partitionBy('user_uid').orderBy(-F.col('source'))
    result = (
        m_prev
        .withColumn('source', F.lit(1))
        .union(m_cur.withColumn('source', F.lit(2)))
        .withColumn('rn', row_number().over(w))
        .filter('rn = 1')
        .drop('rn', 'source')
    )
    logger.info('Users added')
    return result


def save_data(df: DataFrame, ext: str = 'json', path: str = 'output.json') -> None:
    logger.info(f'Saving data - {res_prev.count()} rows')
    df.write.mode('overwrite').format(ext).save(path)
    logger.info('Data saved')


if __name__ == '__main__':
    spark = SparkSession\
        .builder\
        .master('spark://178.205.83.208:7077')\
        .appName('UsersDataMart')\
        .getOrCreate()
    res_prev = None
    for d in [1, 2, 3]:
        logger.info(f'Loading day {d}')
        new_data = generate_data(users=50, day=d)
        res_cur = get_result_mart(*create_marts(new_data))
        if res_prev is not None:
            res_prev = combine_days(res_prev, res_cur)
        else:
            res_prev = res_cur
    save_data(res_prev)
