# Filter Open Observatory of Network Interference (OONI) S3 Keys 

## Overview

This app is the first of three that fits into a data pipeline for a larger project, an Open Observatory of Network Intereference (OONI) Data Lake project. 

## Purpose

The purpose of this simple app is to extract S3 keys of JSONL from OONI's S3 bucket, filter them for target OONI network tests, then load the filtered S3 keys into the destination S3 bucket as text-based `.dat` files organized by year and month.

This app takes care of the data in the top third of the diagram that follows:

![captsone project architecture](img/UdacityCapstoneProject.svg "capstone project architecture")
