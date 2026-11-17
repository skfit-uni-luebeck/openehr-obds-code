# MeDIC-ETL-Toolkit

A toolkit for mapping various data formats to openEHR. It reads json/xml files from kafka, transforms them using groovy scripts to openEHR compositions, which then will be uploaded to the openEHR repository.

This project was initially created to map [oBDS](https://www.basisdatensatz.de/basisdatensatz) (German oncological basis data set) data to openEHR using a FHIR Terminology Server and CentraXX MDR. The target openEHR templates are available [here](https://ckm.highmed.org/ckm/projects/1246.152.61).

## Get started

To get started, copy the `settings.yaml.example` file to `settings.yaml`. This avoids future conflicts within the git tree. The parameters need to be tailored to your environment.

### Running in container

MeDIC-ETL-Toolkit is also available as a container. The image is available at `ghcr.io/skfit-uni-luebeck/medic-etl-toolkit:latest`. There are native builds available for `linux/amd64` and `linux/arm64/v8`.
There is also a compose file available.

## Server availability checks

The toolkit uses exactly three servers and checks each of them separately:

| Server | Settings key | Checked by |
|---|---|---|
| openEHR server | `openEhrUrl` | AQL query `SELECT e/ehr_id/value FROM EHR e LIMIT 1` |
| FHIR Server (data) | `fhirServerUrl` | `GET {base}/metadata` |
| FHIR Terminology Server | `fhirTsUrl` | `GET {base}/metadata` |

A server counts as available when it answers with an HTTP 2xx response; a 4xx/5xx answer or a
connection failure marks it unavailable. Every log message names the server and its URL, so the
FHIR Server and the FHIR Terminology Server can be told apart even if both settings point to the
same host:

```
INFO  Availability checks: 1 openEHR server + 2 FHIR servers, checked when data arrives
INFO    openEHR server           https://ehr.example.org/ehr/
INFO    FHIR Server              https://fhir.example.org/fhir/
INFO    FHIR Terminology Server  https://ontoserver.example.org/fhir/
WARN  FHIR Terminology Server [https://ontoserver.example.org/fhir/] is now unavailable (HTTP 403)
WARN  Pausing processing until all servers are available. Unavailable: FHIR Terminology Server [https://ontoserver.example.org/fhir/] (HTTP 403). Available: openEHR server [https://ehr.example.org/ehr/], FHIR Server [https://fhir.example.org/fhir/]
INFO  FHIR Terminology Server [https://ontoserver.example.org/fhir/] is now available
INFO  Resuming processing after 47s: all servers are available again: openEHR server [...], FHIR Server [...], FHIR Terminology Server [...]
```

The checks are only performed when there is actually data to process: an idle toolkit does not
probe at all, and a successful check is trusted for `intervalMs` before the servers are probed
again.

**If a server is unavailable the toolkit stops processing and waits — it does not skip records,
does not write them to the error topic and does not keep logging errors.** It logs once when
processing is paused (naming the unavailable servers and the reason), retries the servers every
`intervalMs` at debug level, and logs once when all servers answer again and processing
continues. Records are only committed after they were processed, so nothing is lost while the
toolkit waits. If a server goes down while a record is being processed, the servers are checked
freshly (never from a cached verdict): a server problem pauses the record, a problem with the
record itself is still written to the error topic.

`GET /health` reports the current verdict of every server:

```json
{"msgsPerMinute": 12, "cacheSize": {"lookup": 3, "conceptMap": 1}, "servers": {
  "openEhrServer": {"url": "https://ehr.example.org/ehr/", "configured": true, "available": true,
                     "state": "ok", "lastCheckEpochMs": 1763387941012, "lastError": null},
  "fhirServer": {"url": "https://fhir.example.org/fhir/", "configured": true, "available": true,
                 "state": "ok", "lastCheckEpochMs": 1763387941087, "lastError": null},
  "fhirTerminologyServer": {"url": "https://ontoserver.example.org/fhir/", "configured": true,
                            "available": false, "state": "down", "lastCheckEpochMs": 1763387941201,
                            "lastError": "HTTP 403"}}}
```

`state` is `ok`, `down`, `unknown` (not checked yet, e.g. because no data arrived) or
`not-configured`.

Settings (`serverCheck` in `settings.yml`):

| Key | Default | Meaning |
|---|---|---|
| `enabled` | `true` | switch the checks off entirely |
| `intervalMs` | `5000` | pause between repeated checks and validity of a positive result |
| `timeoutMs` | `0` | log a reminder at this interval while waiting; `0` logs only the pause and the resume message |
| `probeTimeoutMs` | `5000` | maximum duration of a single check attempt |
| `retries` | `3` | attempts per server before it is declared unavailable |

Waiting for a server longer than the Kafka consumer's `max.poll.interval.ms` makes the broker
rebalance the consumer group; the uncommitted records are simply redelivered once the servers
answer again. Set `enabled: false` to switch the checks off completely.

## Citation

This work was published and presented at the 34th [Medical Informatics Europe Conference (EFMI)](https://efmi.org/2023/12/16/34th-medical-informatics-europe-conference-mie2024-athens-greece-25-to-29-august-2024/).

[![DOI:10.3233/SHTI240655](https://img.shields.io/badge/DOI-10.3233%2FSHTI240655-brightgreen.svg)](http://dx.doi.org/10.3233/SHTI240655)

```
@incollection{Schladetzky_Reimer_2024,
	title = {Integration of Oncological Data into {openEHR}: A Path Towards Improved Cancer Care and Research},
	rights = {https://creativecommons.org/licenses/by-nc/4.0/},
	isbn = {9781643685335},
	url = {https://ebooks.iospress.nl/doi/10.3233/SHTI240655},
	shorttitle = {Integration of Oncological Data into {openEHR}},
	booktitle = {Studies in Health Technology and Informatics},
	publisher = {{IOS} Press},
	author = {Schladetzky, Jan and Reimer, Niklas and Nicolaus, Herwig and Busch, Hauke and Schreiweis, Björn and Kock-Schoppenhauer, Ann-Kristin},
	editor = {Mantas, John and Hasman, Arie and Demiris, George and Saranto, Kaija and Marschollek, Michael and Arvanitis, Theodoros N. and Ognjanović, Ivana and Benis, Arriel and Gallos, Parisis and Zoulias, Emmanouil and Andrikopoulou, Elisavet},
	urldate = {2024-09-03},
	date = {2024-08-22},
	doi = {10.3233/SHTI240655},
}
```

## License

GNU Affero General Public License v3 (AGPL-3.0)

## Acknowledgment

This research was funded by the German Federal Ministry of Education and Research (BMBF), grant numbers 01KX2121, 01ZZ1801A.