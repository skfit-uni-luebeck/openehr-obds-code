# oBDS 2 openEHR

This project is used to map [oBDS](https://www.basisdatensatz.de/basisdatensatz) (German oncological basis data set) data to openEHR using a FHIR Terminology Server and CentraXX MDR. The target openEHR templates are available [here](https://ckm.highmed.org/ckm/projects/1246.152.61).

## Get started

To get started, copy the `settings.yaml.example` file to `settings.yaml`. This avoids future conflicts within the git tree. The parameters need to be tailored to your environment.

### Running in container

oBDS 2 openEHR is also available as a container. The image is available at `ghcr.io/skfit-uni-luebeck/obds2openehr:latest`. There are native builds available for `linux/amd64` and `linux/arm64/v8`.
There is also a compose file available.

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