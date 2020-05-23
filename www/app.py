import logging
import asyncio
from aiohttp import web


async def index(request):
    return web.Response(body='<h1>Index</h1>'.encode('utf-8'), content_type='text/html')


async def hello(request):
    name = request.match_info['name']
    text = '<h1>hello, %s</h1>' % name
    return web.Response(body=text.encode('utf-8'), content_type='text/html')


async def init():
    app = web.Application()
    app.router.add_routes([web.get('/', index),
                           web.get('/hello/{name}', hello)])
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, 'localhost', 9000)
    await site.start()
    logging.info('server started at http://127.0.0.1:9000...')


logging.basicConfig(level=logging.INFO)
loop = asyncio.get_event_loop()
task = asyncio.ensure_future(init())
loop.run_until_complete(task)
loop.run_forever()
