import argparse
import json
import logging
import time
from typing import Any, Dict, List


import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
import apache_beam.transforms.window as window

PROJECT = "PROJECT NAME"

TOPIC = "projects/PROJECT NAME/topics/TOPIC NAME"

raw_activity_table = "PROJECT NAME:DATASET.raw_activity_table" 

activity_summary_table = "PROJECT NAME:DATASET.activity_summary_table"



# Defines the BigQuery schema for the output table.
ACTIVITY_SUMMARY_SCHEMA = ",".join(
    [
        "eventTimeMinutes:STRING",
        "View:INTEGER",
        "Delete:INTEGER",
        "Create:INTEGER",
        "Modify:INTEGER",
    ]
)

RAW_SCHEMA = ",".join(
    [
        "activity:STRING",
        "object:STRING",
        "username:STRING",
        "team:STRING",
        "location:STRING",
	"eventTime:STRING",
	"eventTimeMinutes:STRING",
	"processingTime:TIMESTAMP"
		
    ]
)

class PivotandRemoveNull(beam.DoFn):
    
    def process(self, e):

        from itertools import groupby

        result = []
        for _, value in groupby(sorted(e, key=lambda x :x['eventTimeMinutes']),
                        key= lambda x: x['eventTimeMinutes']):
            temp = {}
            for d in value:
                temp.update(**d)
            result.append(temp)
        for x in result:
            if 'NULL' in x:
                del x['NULL']
        return result
        
    

class Parse(beam.DoFn):

    def process(self, m):

        import datetime as dt
        import time 
        
        e = json.loads(m)
        
        return [{ 
            'activity': e['activity'],
            'object': e['object'],
            'username': 'NULL' if e['profile'] == 'NULL' else e['profile']['username'],
            'team': 'NULL' if e['profile'] == 'NULL' else e['profile']['team'],
            'location': 'NULL' if e['profile'] == 'NULL' else e['profile']['location'],
            'eventTime': e['time'],
            'eventTimeMinutes': dt.datetime.strptime(e['time'], "%Y-%m-%d %H:%M:%S").strftime("%Y-%m-%d %H:%M"),
            'processingTime': int(time.time())
        }]
		
def run(
   beam_args: List[str] = None,
) -> None:
    """Build and run the pipeline."""
    with beam.Pipeline(options=PipelineOptions()) as p:
        Raw = (
            p 
            | "Read from Pub/Sub" >> beam.io.ReadFromPubSub(topic=TOPIC)
            | 'Rename Columns' >> beam.ParDo(Parse())
            )
        RawToBigquery = Raw | "Write Raw to Big Query" >> beam.io.WriteToBigQuery(raw_activity_table, schema=RAW_SCHEMA, write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND)
        ObjectSummary = (
            Raw
            |"Fixed-size windows" >> beam.WindowInto(beam.window.FixedWindows(30,0))
            |'Group by Minutes and Activity' >> beam.Map(lambda x: (x['eventTimeMinutes'] + "," + x['activity'] ,x))
            |'Count Perkey' >> beam.combiners.Count.PerKey()
            |"Map Tuple to Dictionary" >> beam.MapTuple(
                lambda eventTimeMinutes, count:{
                    "eventTimeMinutes": eventTimeMinutes.split(",")[0],
                    eventTimeMinutes.split(",")[1]: count 
                }
            )
	    |"Convert to List" >> beam.CombineGlobally(beam.combiners.ToListCombineFn()).without_defaults()
	    |"Pivot and Remove Null" >> beam.ParDo(PivotandRemoveNull())
        )
        SummaryToBigquery = ObjectSummary |"Write Summary to Big Query" >> beam.io.WriteToBigQuery(activity_summary_table, schema=ACTIVITY_SUMMARY_SCHEMA, write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND)



if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    parser = argparse.ArgumentParser()
    args, beam_args = parser.parse_known_args()
    run()
