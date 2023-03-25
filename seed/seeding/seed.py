import logging
import argparse
from reseedingDynamo import create_dynamo_table, populate_dynamo_table, delete_dynamo_data

# logger, must be treated as a singleton or repeated logs will appear
logger = logging.getLogger()
logger.setLevel(logging.INFO)
logger.addHandler(logging.StreamHandler())

parser = argparse.ArgumentParser()
parser.add_argument('--profile', dest='profile')
args = parser.parse_args()

if args.profile == 'dynamo' or args.profile == 'run':
    logger.info(f'Implementing -> {args.profile}')
    if args.profile == 'dynamo':
        delete_dynamo_data(logger)
        create_dynamo_table(logger)
        populate_dynamo_table(logger)
    elif args.profile == 'run':
        pass



