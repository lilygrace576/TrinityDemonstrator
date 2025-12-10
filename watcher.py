import time
import logging
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
import os
import argparse


class OnMyWatch:
    def __init__(self, watch_dir, output_dir, timeout=300):
        self.watchDirectory = watch_dir
        self.outputDirectory = output_dir
        self.timeout = timeout
        self.observer = Observer()

    def run(self):
        event_handler = Handler(self.outputDirectory)
        self.observer.schedule(event_handler, self.watchDirectory, recursive=False)
        self.observer.start()
        try:
            while True:
                time.sleep(5)
                # check if timeout expired
                if time.time() - event_handler.last_event_time > self.timeout:
                    print(f"No new files observed in {self.timeout} seconds, stopping watcher.")
                    break
        finally:
            self.observer.stop()
            logging.info("Observer Stopped")

        self.observer.join()


class Handler(FileSystemEventHandler):
    def __init__(self, output_dir):
        self.last_event_time = time.time()
        self.outputDirectory = output_dir

    def on_any_event(self, event):
        if event.is_directory:
            return None

        if (event.event_type in ['created', 'modified']) and event.src_path.endswith(".root"):
            self.last_event_time = time.time()
            logging.info(f"Watchdog received an event - {event.src_path}")
            eventPath = event.src_path

            # Extract date string from filename
            location = eventPath.find("AsAd0_")
            if location != -1:
                dateStr = eventPath[location+6:location+16].replace('-', '')
                output_path = os.path.join(self.outputDirectory, dateStr)
                os.makedirs(output_path, exist_ok=True)
                try:
                    os.system(f'mv "{eventPath}" "{output_path}/"')
                    logging.info(f"Moved {eventPath} to {output_path}")
                except Exception as e:
                    logging.info(f"Failed to move {eventPath} to {output_path}: {e}")
            else:
                logging.info(f"Filename did not contain expected pattern: {eventPath}")


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '-l', '--log',
        type=str,
        default='/mnt/DataAnalysis/MergedData/.logs/watcher.log',
        help='Path to log file'
    )
    parser.add_argument(
        '-i', '--input',
        type=str,
        default='/mnt/DataAnalysis/MergedData/',
        help='Directory to watch for new files'
    )
    parser.add_argument(
        '-o', '--output',
        type=str,
        default='/mnt/DataAnalysis/MergedData/Output/',
        help='Directory to move files into'
    )
    parser.add_argument(
        '-t', '--timeout',
        type=int,
        default=300,
        help='Timeout in seconds with no new files before exiting'
    )

    args = parser.parse_args()

    logging.basicConfig(
        filename=args.log,
        filemode='w',
        level=logging.INFO,
        format='%(asctime)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )

    logging.info("Watchdog started monitoring...")
    watch = OnMyWatch(args.input, args.output, args.timeout)
    watch.run()