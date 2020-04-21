# Crawler + Similarity

## How to using Google Colab

1. Create new colab notebook
2. Rename your colab notebook (I dont think you want untitled notebook)
3. Mount google drive
4. cd to Colab Notebook dir on drive
    `%cd drive/My Drive/Colab Notebooks`
5. Clone this repo
    `!git clone https://github.com/rpahlevy/crawl_sim`
6. Download miniconda & change permission to execute
    `!wget https://repo.continuum.io/miniconda/Miniconda3-py37_4.8.2-Linux-x86_64.sh && chmod +x Miniconda3-py37_4.8.2-Linux-x86_64.sh`
7. Install miniconda
    `!bash ./Miniconda3-py37_4.8.2-Linux-x86_64.sh -b -f -p /usr/local`
8. Install SCRAPY (dont forget to input 'y')
    `!conda install -c conda-forge scrapy=1.5.2=py37_0`
9. Install SPACY (dont forget to input 'y')
    `!conda install -c conda-forge spacy`
10. Download SPACY package LARGE, because why not, you are using Google's resources
    `!python -m spacy download en_core_web_lg`
11. RUN!
    `!cd crawl_sim/ && scrapy crawl crawler`