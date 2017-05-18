from search.index.adapter.user_adapter import UserAdapter


def main():
    u = UserAdapter()
    u.listen_and_update_index()


if __name__ == "__main__":
    main()
