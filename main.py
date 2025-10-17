from source.modules.intersectNdvi.insertNdviDataIntoDatabase import (
    InsertIntersectNdviIntoDatabase)


if __name__ == "__main__":
    InsertIntersectNdviIntoDatabase(
        safra_list=[2025],
        janela_list=['J3'],
        clients_id=[58],
        clients_to_remove=[],
        clients_folder='D:/SIGMA X/CORURIPE/2025/Tomo4Lite/'
    ).main()
