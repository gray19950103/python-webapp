import asyncio
import aiomysql
import logging
import datetime

logging.basicConfig(
	level=logging.INFO,
	format='%(asctime)s - %(filename)s[line:%(lineno)d] - %(levelname)s: %(message)s'
)


async def createPool(event_loop, **kwargs):
	logging.info('creating database connection pool...')
	global __pool
	__pool = await aiomysql.create_pool(
		host=kwargs.get('host', '127.0.0.1'),
		port=kwargs.get('port', 3306),
		user=kwargs.get('user', 'root'),
		password=kwargs.get('password', 'password'),
		db=kwargs.get('db', 'mysql'),
		charset=kwargs.get('charset', 'utf8'),
		autocommit=kwargs.get('autocommit', True),
		minsize=kwargs.get('minsize', 1),
		maxsize=kwargs.get('maxsize', 10),
		loop=event_loop
	)


async def executeSql(sql, args, select=True, limit=None):
	logging.info(sql.replace('?', '%s') % tuple(args))
	global __pool
	async with __pool.acquire() as conn:
		cur = await conn.cursor(aiomysql.DictCursor)
		if select:
			await cur.execute(sql.replace('?', '%s'), args or ())
			if limit:
				result = await cur.fetchmany(limit)
				logging.info('rows returned: %s' % len(result))
			else:
				result = await cur.fetchall()
				logging.info('rows returned: %s' % len(result))
			logging.info('closing cursor...')
			await cur.close()
			return result
		else:
			await cur.execute(sql.replace('?', '%s'), args)
			affected = cur.rowcount
			logging.info('rows affected: %s' % affected)
			logging.info('closing cursor...')
			await cur.close()
			return affected


class Field(object):
	def __init__(self, name, column_type, primary_key, default):
		self.name = name
		self.column_type = column_type
		self.primary_key = primary_key
		self.default = default

	def __str__(self):
		return '<%s, %s:%s>' % (self.__class__.__name__, self.column_type, self.name)


class StringField(Field):
	def __init__(self, name=None, primary_key=False, default=None, ddl='varchar(100)'):
		super().__init__(name, ddl, primary_key, default)


class BooleanField(Field):
	def __init__(self, name=None, default=False):
		super().__init__(name, 'boolean', False, default)


class IntegerField(Field):
	def __init__(self, name=None, primary_key=False, default=0):
		super().__init__(name, 'bigint', primary_key, default)


class FloatField(Field):
	def __init__(self, name=None, primary_key=False, default=0.0):
		super().__init__(name, 'real', primary_key, default)


class TextField(Field):
	def __init__(self, name=None, default=None):
		super().__init__(name, 'text', False, default)


class ModelMetaclass(type):

	def __new__(mcs, name, bases, attrs):
		# 排除Model类本身:
		if name == 'Model':
			return type.__new__(mcs, name, bases, attrs)
		# 获取table名称:
		tableName = attrs.get('__table__', None) or name
		logging.info('found model: %s (table: %s)' % (name, tableName))
		# 获取所有的Field和主键名:
		mappings = dict()
		fields = []
		primaryKey = None
		for k, v in attrs.items():
			if isinstance(v, Field):
				logging.info('  found mapping: %s ==> %s' % (k, v))
				mappings[k] = v
				if v.primary_key:
					# 找到主键:
					if primaryKey:
						raise RuntimeError('Duplicate primary key for field: %s' % k)
					primaryKey = k
				else:
					fields.append(k)
		if not primaryKey:
			raise RuntimeError('Primary key not found.')
		for k in mappings.keys():
			attrs.pop(k)
		escaped_fields = list(map(lambda f: '`%s`' % f, fields))
		attrs['__mappings__'] = mappings  # 保存属性和列的映射关系
		attrs['__table__'] = tableName
		attrs['__primary_key__'] = primaryKey  # 主键属性名
		attrs['__fields__'] = fields  # 除主键外的属性名
		# 构造默认的SELECT, INSERT, UPDATE和DELETE语句:
		attrs['__select__'] = 'select `%s`, %s from `%s`' % (primaryKey, ', '.join(escaped_fields), tableName)
		attrs['__insert__'] = 'insert into `%s` (%s, `%s`) values (%s)' % (
			tableName, ', '.join(escaped_fields), primaryKey, ', '.join('?' * (len(escaped_fields) + 1)))
		attrs['__update__'] = 'update `%s` set %s where `%s`=?' % (
			tableName, ', '.join(map(lambda f: '`%s`=?' % (mappings.get(f).name or f), fields)), primaryKey)
		attrs['__delete__'] = 'delete from `%s` where `%s`=?' % (tableName, primaryKey)
		return type.__new__(mcs, name, bases, attrs)


class Model(dict, metaclass=ModelMetaclass):
	def __init__(self, **kw):
		super(Model, self).__init__(**kw)

	def __getattr__(self, key):
		try:
			return self[key]
		except KeyError:
			raise AttributeError(r"'Model' object has no attribute '%s'" % key)

	def __setattr__(self, key, value):
		self[key] = value

	def getValue(self, key):
		return getattr(self, key, None)

	def getValueOrDefault(self, key):
		value = getattr(self, key, None)
		if value is None:
			field = self.__mappings__[key]
			if field.default is not None:
				value = field.default() if callable(field.default) else field.default
				logging.debug('using default value for %s: %s' % (key, str(value)))
				setattr(self, key, value)
		return value

	@classmethod
	async def find(cls, pk):
		""" find object by primary key. """
		result = await executeSql('%s where %s= ?' % (cls.__select__, cls.__primary_key__), [pk], select=True,  limit=1)
		if len(result) == 0:
			return None
		return result

	@classmethod
	async def findAll(cls, where=None, limit=None):
		""" find all objects that satisfy where conditions. """
		if where:
			result = await executeSql('%s where %s' % (cls.__select__, where), [], select=True, limit=limit)
		else:
			result = await executeSql(cls.__select__, [], select=True, limit=limit)
		if len(result) == 0:
			return None
		return result

	@classmethod
	async def findCount(cls, where=None, limit=None):
		""" get count of objects that satisfy where conditions. """
		result = await cls.findAll(where=where, limit=limit)
		return len(result)

	async def save(self):
		""" save user object into user table. """
		args = list(map(self.getValueOrDefault, self.__fields__))
		args.append(self.getValueOrDefault(self.__primary_key__))
		try:
			result = await executeSql(self.__insert__, args, select=False)
			if result != 1:
				logging.warning('failed to insert record. affected rows: %s' % result)
		except Exception as e:
			logging.error(e)

	async def update(self):
		""" update user object info on user table. """
		args = list(map(self.getValueOrDefault, self.__fields__))
		args.append(self.getValueOrDefault(self.__primary_key__))
		result = await executeSql(self.__update__, args, select=False)
		if result != 1:
			logging.warning('failed to update record. affected rows: %s' % result)

	@classmethod
	async def remove(cls, primary):
		""" remove user record on user table. """
		result = await executeSql(cls.__delete__, [primary], select=False)
		if result != 1:
			logging.warning('failed to remove record. affected rows: %s' % result)


class Student(Model):
	__table__ = 'Student'

	SId = IntegerField(primary_key=True)
	Sname = StringField()
	Sage = StringField()
	Ssex = StringField()


global __pool
loop = asyncio.get_event_loop()
task_1 = asyncio.ensure_future(
	createPool(
		event_loop=loop,
		host='101.132.132.220',
		password='Theapple1995',
		db='education'
	)
)
logging.info('start task_1...')
loop.run_until_complete(task_1)

# task_2 = asyncio.ensure_future(Student.find('02'))
# loop.run_until_complete(task_2)
# print(task_2.result())

# s = Student(SId='08', Sname='王菊', Sage=datetime.datetime(1990, 10, 1, 0, 0), Ssex='女')
# task_3 = asyncio.ensure_future(s.save())
# loop.run_until_complete(task_3)

# task_4 = asyncio.ensure_future(Student.findAll(where='SId >= 02'))
# loop.run_until_complete(task_4)
# print(task_4.result())
# #
# task_5 = asyncio.ensure_future(Student.findCount(where='SId >= 12'))
# loop.run_until_complete(task_5)
# print(task_5.result())

# s = Student(SId='08', Sname='王菊', Sage=datetime.datetime(1990, 10, 1, 0, 0), Ssex='女')
# task_6 = asyncio.ensure_future(s.update())
# loop.run_until_complete(task_6)

# task_6 = asyncio.ensure_future(Student.remove('08'))
# loop.run_until_complete(task_6)
