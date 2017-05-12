from connector.user_connector import UserConnector


def main():
    u = UserConnector()
    u.listen()


if __name__ == "__main__":
    main()
