import asyncio
import logging
import subprocess
from concurrent.futures import ThreadPoolExecutor
from http import HTTPStatus
from datetime import datetime
from uuid import UUID, uuid4
from typing import Dict

import requests
from requests.auth import HTTPBasicAuth
from fastapi import BackgroundTasks
from fastapi import FastAPI
from pydantic import BaseModel, Field

# Logger initialization
format = "%(asctime)s: %(message)s"
logging.basicConfig(format=format, level=logging.INFO,
                        datefmt="%H:%M:%S")

#pydantic model definitions
class ChannelData(BaseModel):
    keyname: str
    name: str
    type: str
    stream_url: str


class Job(BaseModel):
    uid: UUID = Field(default_factory=uuid4)
    status: str = "in_progress"
    result: int = None
    data: ChannelData

app = FastAPI()
jobs: Dict[UUID, Job] = {}


# records an audio file for 60 seconds and saves it
def record(channel):
    date = datetime.today().strftime('%Y%m%d_%H%M%S')
    name = channel['keyname']
    filename = f'recordings/{name}_server01_{date}.aac'
    start_time = datetime.today()
    logging.info(f"Thread {filename}: started | Time:{start_time}")
    
    p = subprocess.Popen(['ffmpeg', '-i', channel["stream_url"], '-acodec', 'aac', '-ab', '48000', 
                        '-ar', '22050', '-ac', '1', '-t', '60', 
                        filename],  stderr=subprocess.PIPE, stdout=subprocess.PIPE)
    output, error = p.communicate()

    error = error.decode('UTF-8')
    if p.returncode != 0:
        logging.info(f"Thread {filename}: ERROR | {error}")
        return  None

    end_time = datetime.today()
    logging.info(f"Thread {filename}: finished | Time:{end_time}")
    result = {}
    result['end_time'] = str(end_time)
    result['channel_id'] = name
    result['start_time'] = str(start_time)
    result['path'] = filename
    logging.info(f"Thread {filename}: upload status: {response}")
    return result


async def run_in_process(fn, *args):
    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(app.state.executor, fn, *args)  # wait and return result


async def start_task(uid: UUID, data: dict) -> None:
    jobs[uid].result = await run_in_process(record, data)
    jobs[uid].status = "complete"


@app.post("/record", status_code=HTTPStatus.ACCEPTED)
async def task_handler(data: ChannelData, background_tasks: BackgroundTasks):
    new_task = Job()
    jobs[new_task.uid] = new_task
    jobs[new_task.uid].data = data
    background_tasks.add_task(start_task, new_task.uid, data.dict())
    return new_task


@app.get("/status/{uid}")
async def status_handler(uid: UUID):
    return jobs[uid]


@app.on_event("startup")
async def startup_event():
    app.state.executor = ThreadPoolExecutor()
    logging.info(f"Threadpool starting")


@app.on_event("shutdown")
async def on_shutdown():
    logging.info(f"Threadpool shutting down")
    app.state.executor.shutdown()