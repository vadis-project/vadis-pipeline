<!-- This template is adapated from https://github.com/othneildrew/Best-README-Template -->

<!-- PROJECT SHIELDS -->
<!--
*** I'm using markdown "reference style" links for readability.
*** Reference links are enclosed in brackets [ ] instead of parentheses ( ).
*** See the bottom of this document for the declaration of the reference variables
*** for contributors-url, forks-url, etc. This is an optional, concise syntax you may use.
*** https://www.markdownguide.org/basic-syntax/#reference-style-links
-->
[![Issues][issues-shield]][issues-url]
[![MIT License][license-shield]][license-url]



<!-- PROJECT LOGO -->
<br />
<div align="center">
  <h3 align="center">VADIS Pipeline</h3>

  <p align="center">
    Main pipeline containing essential parts of the VADIS Project.
    <br />
    <br />
    <a href="https://demo-vadis.gesis.org/">View Demo</a>
    ·
    <a href="TODO: add project github followed by: /issues/new?labels=bug&template=bug-report---.md">Report Bug</a>
    ·
    <a href="TODO: add project github followed by: /issues/new?labels=enhancement&template=feature-request---.md">Request Feature</a>
  </p>
</div>



<!-- TABLE OF CONTENTS -->
<details>
  <summary>Table of Contents</summary>
  <ol>
    <li>
      <a href="#about-the-project">About The Project</a>
    </li>
    <li>
      <a href="#getting-started">Getting Started</a>
      <ul>
        <li><a href="#prerequisites">Prerequisites</a></li>
        <li><a href="#installation">Installation</a></li>
      </ul>
    </li>
    <li><a href="#usage">Usage</a></li>
    <li><a href="#roadmap">Roadmap</a></li>
    <li><a href="#license">License</a></li>
    <li><a href="#contact">Contact</a></li>
    <li><a href="#contact">Citation</a></li>
  </ol>
</details>

<!-- ABOUT THE PROJECT -->
## About The Project

![VADIS Pipeline](https://github.com/vadis-project/vadis-pipeline/blob/main/readme/pipeline.png)


This project provides an in-depth look at the main pipeline of the VADIS Project. The process initiates with the selection and crawling of datasets and publications. These documents are then preprocessed to ensure the data is clean and structured for the main tasks. The main tasks of VADIS are Variable Identification and Summarization are executed and their outputs are used to construct VADIS Data. VADIS Data is then forwarded to VADIS Demo. Details of the each process are as followed:

### (External) Data Retrieval

* Publication Selection (p0): This process uses a predefined queries (config) to retrieve SSOAR publications with related research datasets from GESIS Search Index (config). Query results are saved in corpus.

* Publication and Dataset Crawling (p1): For each publication in the query results, 

### Preprocessing

* PDF Parsing

* Text Processing

### VADIS Tasks

* Summarization

* Variable Identification

### Output - VADIS Data

* Merge and Format



<!-- GETTING STARTED -->
## Getting Started

TODO: add details on how to use this repository

### Prerequisites

TODO: describe prerequisites
* python
  ```sh
  npm install npm@latest -g
  ```

### Installation


1. Clone the repo
   ```sh
   git clone https://github.com/vadis-project/vadis-pipeline.git
   ```
2. Create and activate a virtual environment
   ```sh
   python3 -m venv venv
   source /venv/bin/activate
   ```
3. Install python packages
   ```sh
   pip install -r requirements.txt
   ```


<!-- USAGE EXAMPLES -->
## Usage

TODO: describe how to use the tool


<!-- ROADMAP -->
## Roadmap

TODO: add roadmap if it applies

- [ ] Feature 1
- [ ] Feature 2

<!-- LICENSE -->
## License

See `LICENSE` for more information.

<!-- CONTACT -->
## Contact

Yavuz Selim Kartal - yavuzselim.kartal@gesis.org

## Citation
```bibtex
@misc{kartal2023vadis,
      title={VADIS -- a VAriable Detection, Interlinking and Summarization system}, 
      author={Yavuz Selim Kartal and Muhammad Ahsan Shahid and Sotaro Takeshita and Tornike Tsereteli and Andrea Zielinski and Benjamin Zapilko and Philipp Mayr},
      year={2023},
      eprint={2312.13423},
      archivePrefix={arXiv},
      primaryClass={cs.DL}
}
```

<!-- MARKDOWN LINKS & IMAGES -->
<!-- https://www.markdownguide.org/basic-syntax/#reference-style-links -->
[issues-shield]: https://img.shields.io/github/issues/othneildrew/Best-README-Template.svg?style=for-the-badge
[issues-url]: https://github.com/othneildrew/Best-README-Template/issues
[license-shield]: https://img.shields.io/github/license/othneildrew/Best-README-Template.svg?style=for-the-badge
[license-url]: https://github.com/othneildrew/Best-README-Template/blob/master/LICENSE
[product-screenshot]: images/screenshot.png
