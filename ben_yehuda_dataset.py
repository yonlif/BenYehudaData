from pathlib import Path
import json
import csv
from torch.utils.data import Dataset
import string
import matplotlib.pyplot as plt
from tqdm import tqdm


class BenYehudaDataset(Dataset):
    def __init__(self, 
                 pseudocatalogue_path: str,
                 authors_dir: str,
                 txt_dir: str,
                 encoding: str = 'utf-8'):
        self.samples = []
        self.author_years = {}
        self.txt_dir = Path(txt_dir)
        self.encoding = encoding

        authors_dir = Path(authors_dir)
        pseudocatalogue_path = Path(pseudocatalogue_path)

        # Load author birth/death years
        for author_file in authors_dir.glob('author_*.json'):
            with author_file.open(encoding=encoding) as f:
                data = json.load(f)
                author_id = int(data['id'])
                person = data['metadata'].get('person', {})
                birth = person.get('birth_year')
                death = person.get('death_year')
                if birth and death:
                    try:
                        birth = int(birth)
                        death = int(death)
                    except ValueError:
                        print(f"Invalid year format for author {author_id}: {birth}, {death}")
                        continue
                    self.author_years[author_id] = (int(birth), int(death))

        # Read pseudocatalogue.csv
        with pseudocatalogue_path.open(encoding=encoding) as f:
            reader = csv.DictReader(f)
            for row in reader:
                path = row.get('path', '').strip()
                author_id = int(path.split('/')[1][1:])
                if not author_id or not path:
                    continue
                if path.startswith('/'):
                    path = path[1:]
                txt_path = self.txt_dir / f'{path}.txt'
                if author_id in self.author_years and txt_path.is_file():
                    self.samples.append((txt_path, self.author_years[author_id]))
                else:
                    print(f"Skipping {txt_path} because it doesn't exist or author_id {author_id} is not in author_years")

    def __len__(self):
        return len(self.samples)

    def __getitem__(self, idx):
        txt_path, years = self.samples[idx]
        with txt_path.open(encoding=self.encoding) as f:
            text = f.read()
        return text, years

def plot_dataset_statistics(text_lengths, birth_years, death_years):
    plt.figure(figsize=(15, 4))
    plt.subplot(1, 3, 1)
    plt.hist(text_lengths, bins=50, color='skyblue', edgecolor='black')
    plt.title('Text Length Distribution')
    plt.xlabel('Text Length (characters)')
    plt.ylabel('Count')

    plt.subplot(1, 3, 2)
    plt.hist(birth_years, bins=30, color='lightgreen', edgecolor='black')
    plt.title('Birth Year Distribution')
    plt.xlabel('Birth Year')
    plt.ylabel('Count')

    plt.subplot(1, 3, 3)
    plt.hist(death_years, bins=30, color='salmon', edgecolor='black')
    plt.title('Death Year Distribution')
    plt.xlabel('Death Year')
    plt.ylabel('Count')

    plt.tight_layout()
    plt.show()

def print_dataset_statistics(dataset):
    print(f"Number of samples: {len(dataset)}")
    text_lengths = []
    birth_years = []
    death_years = []
    total_words = 0
    total_chars = 0
    for i in tqdm(range(len(dataset))):
        text, years = dataset[i]
        birth, death = years
        birth_years.append(birth)
        death_years.append(death)
        text_lengths.append(len(text))
        text_no_punct = text.translate(str.maketrans(string.punctuation, ' '*len(string.punctuation)))
        total_words += len([tok for tok in text_no_punct.split() if tok.strip()])
        total_chars += len(text)
    print(f"Text lengths: min={min(text_lengths)}, max={max(text_lengths)}, mean={sum(text_lengths)/len(text_lengths):.2f}")
    print(f"Birth years: min={min(birth_years)}, max={max(birth_years)}, mean={sum(birth_years)/len(birth_years):.2f}")
    print(f"Death years: min={min(death_years)}, max={max(death_years)}, mean={sum(death_years)/len(death_years):.2f}")
    print(f"Number of unique authors: {len(set(dataset.author_years.keys()))}")
    print(f"Total number of words: {total_words}")
    print(f"Total number of characters: {total_chars}")
    plot_dataset_statistics(text_lengths, birth_years, death_years)

if __name__ == "__main__":
    dataset = BenYehudaDataset(
        pseudocatalogue_path="public_domain_dump-2025-03/pseudocatalogue.csv",
        authors_dir="scraper/benyehuda_data/authors",
        txt_dir="public_domain_dump-2025-03/txt"
    )
    print_dataset_statistics(dataset)
    
