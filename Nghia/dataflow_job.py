import argparse
import json
import logging
import typing

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.runners import DirectRunner
from beam_nuggets.io import relational_db
from sqlalchemy import Column, Integer, String, Table

# functions and classes

# class Account(typing.NamedTuple):
#     id: int
#     name: str
#     surname: str

# beam.coders.registry.register_coder(Account, beam.coders.RowCoder)


class ConvertToAccountFn(beam.DoFn):
    def process(self, element):
        try:
            row = json.loads(element.decode("utf-8"))

            # yield object Account to write to bigquery
            # yield beam.pvalue.TaggedOutput("parsed_row", Account(**row))

            # dict type can write to postgresql
            yield beam.pvalue.TaggedOutput("parsed_row", row)
        except:
            yield beam.pvalue.TaggedOutput("unparsed_row", element.decode("utf-8"))


def run():

    # Setting up the Beam pipeline options
    options = PipelineOptions(
        save_main_session=True,
        streaming=True,
        direct_running_mode="in_memory",
        direct_num_workers=2,
        runner="DirectRunner",
    )

    
    input_sub = "projects/nttdata-c4e-bde/subscriptions/uc1-input-topic-sub-3"
    table_name = "account"
    dlq_topic = "projects/nttdata-c4e-bde/topics/uc1-dlq-topic-3"


    # postgresql database connection
    source_config = relational_db.SourceConfiguration(
        drivername="postgresql+pg8000",  # postgresql+pg8000
        host="127.0.0.1",
        port=5432,
        username="nttdata",
        password="nttdata",
        database="nttdata",
        create_if_missing=True,  # create the database if not there
    )

    # Table schema for PostgreSQL
    def define_students_table(metadata):
        return Table(
            table_name,
            metadata,
            Column("id", Integer, primary_key=True),
            Column("name", String(100)),
            Column("surname", String(100)),
        )

    table_config = relational_db.TableConfiguration(
        name=table_name, create_if_missing=True, define_table_f=define_students_table
    )

    # Create the pipeline
    p = beam.Pipeline(options=options)

    rows = (
        p
        | "ReadFromPubSub" >> beam.io.ReadFromPubSub(subscription=input_sub)
        | "ParseJson"
        >> beam.ParDo(ConvertToAccountFn()).with_outputs("parsed_row", "unparsed_row")
    )

    (
        rows.unparsed_row
        | "ConvertToByteString" >> beam.Map(lambda row: row.encode("utf-8"))
        | "WriteToDlqTopic"
        >> beam.io.WriteToPubSub(topic=dlq_topic, with_attributes=False)
    )

    (
        rows.parsed_row
        | "WriteToBTable"
        >> relational_db.Write(source_config=source_config, table_config=table_config)
    )

    logging.getLogger().setLevel(logging.INFO)
    logging.info("Building pipeline ...")

    p.run().wait_until_finish()


if __name__ == "__main__":
    run()
