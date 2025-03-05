import asyncio
from src.services.task_scheduler_service import start_scheduler

if __name__ == "__main__":
    try:
        asyncio.run(start_scheduler())
    except RuntimeError as err:
        print(err)
