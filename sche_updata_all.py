import schedule
import time
from update_all_data import *
from update_all_info import *


if __name__ == '__main__':

    schedule.every().monday.at('5:00').do(update_all_info)
    schedule.every().day.at('8:00').do(update_all_data, update_k5=True)
    schedule.every().day.at('21:00').do(update_all_data, update_kd=True)

    while True:
        schedule.run_pending()
        time.sleep(1)
