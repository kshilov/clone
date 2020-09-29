import pickle
import os
import pandas as pd

FILENAME_PANDAS = "./pandas_emails.pkl"

def get_dialog(df):
    res = df[['from', 'to']].dropna()
    count = len(res.index)

    return (res, count)

#from, cc, bcc for such emails are NaN
def get_never_reply_to_me(df):
    res = df[['from', 'cc', 'bcc']]
    res = res.query('(`from` != `from`) & (`cc` != `cc`) & (`bcc` != `bcc`)')

    count = len(res.index)
    return (res, count)

def get_never_answered_by_me(df):
    res = df[['to', 'cc', 'bcc']]
    res = res.query('(`to` != `to`) & (`cc` != `cc`) & (`bcc` != `bcc`)')

    count = len(res.index)
    return (res, count)


def top_20_contacted_by_me(df):
    res = df.sort_values(by=['to'], ascending=False)
    count = len(res.index)

    return (res, count)


def top_20_writing_to_me(df):
    res = df.sort_values(by=['from'], ascending=False)
    count = len(res.index)

    return (res, count)


def main():
    if not os.path.exists(FILENAME_PANDAS):
        print(f"{FILENAME_PANDAS} doesn't exists run extract contacts first")
        exit(0)

    print("...reading data")
    data_df = pd.read_pickle(FILENAME_PANDAS)
    total_emails = len(data_df.index)

    print("...count analytics")
    dialogs, dialogs_count = get_dialog(data_df)
    notreply, notreply_count = get_never_reply_to_me(data_df)
    notanswered, notanswered_count = get_never_answered_by_me(data_df)

    print(f"...Total unique emails={total_emails}, total dialogs={dialogs_count} never_reply_to_me={notreply_count} not_answered_by_myself={notanswered_count}")


if __name__ == '__main__':
    main()