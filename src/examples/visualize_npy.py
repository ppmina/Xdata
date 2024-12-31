import numpy as np

from cryptoservice.data import StorageUtils


def main() -> None:
    data = np.load("data/1h/volume/20240101.npy", allow_pickle=True)
    print(data.shape)


if __name__ == "__main__":
    main()
