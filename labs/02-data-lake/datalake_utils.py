"""Utilidades compartidas del Lab 2 (dadas)."""

import os

import boto3

STACK_NAME = "paytrack-datalake-{stage}"


def get_bucket(stage: str = "dev") -> str:
    """Nombre del bucket del Data Lake: variable DATALAKE_BUCKET o salida del stack."""
    if bucket := os.getenv("DATALAKE_BUCKET"):
        return bucket
    cfn = boto3.client("cloudformation")
    stack = cfn.describe_stacks(StackName=STACK_NAME.format(stage=stage))["Stacks"][0]
    outputs = {o["OutputKey"]: o["OutputValue"] for o in stack.get("Outputs", [])}
    return outputs["DataLakeBucket"]
