from .config import *
from aiogoogle.models import Request
from urllib3.util import parse_url


# gmai api discover: https://www.googleapis.com/discovery/v1/apis/gmail/v1/rest
def get_path(url):
    url = parse_url(url)
    path = url.path
    if url.query:
        path = path + '?' + url.query

    return path


async def gen_list_messages(service,
                            aiogoogle,
                            page_token=None,
                            user_id='me',
                            max_results=MESSAGES_MAX_RESULTS):
    try:
        async with aiogoogle as g:
            request = service.users.messages.list(userId=user_id,
                                                  maxResults=max_results)
            if page_token is not None:
                request = service.users.messages.list(userId=user_id,
                                                      pageToken=page_token,
                                                      maxResults=max_results)

            response = await g.as_user(request,
                                       full_res=True)

            async for next_page in response:
                yield next_page

    except Exception as e:
        print(f"ERROR: {str(e)} the last page_token={page_token}")


async def get_message(service,
                      aiogoogle,
                      msg_id,
                      user_id='me',
                      format='full'):
    async with aiogoogle as g:
        request = service.users.messages.get(userId=user_id,
                                             id=msg_id,
                                             format=format)
        message = await g.as_user(request)

        return message


def get_batch(service, ids, user_id='me', format='full'):
    batch = [
        service.users.messages.get(userId=user_id,
                                   id=i['id'],
                                   format=format) for i in ids
    ]

    return batch


def get_batch_request(*requests,
                      delimiter='batch_foobarxz'):
    header = {}
    header['Content-Type'] = f'multipart/mixed; boundary="{delimiter}"'
    data = ''

    for r in requests:
        path = get_path(r.url)
        data += f'--{delimiter}\nContent-Type: application/http\n\n{r.method} {path}\n\n'

    data += f'--{delimiter}--'

    url = GMAIL_BASE_URL + GMAIL_BATCH_PATH
    batch_request = Request(
        method='POST',
        url=url,
        batch_url=url,
        headers=header,
        data=data
    )

    return batch_request


def get_batch_requests_list(service, ids, per_batch=REQUESTS_PER_BATCH):
    batches_count = round(len(ids) / per_batch)

    requests = []
    for i in range(batches_count):
        start = per_batch * i
        end = start + per_batch

        next_batch = get_batch(service, ids[start:end])
        requests.append(get_batch_request(*next_batch))

    return requests


async def get_batch_response(aiogoogle, batch_request):
    try:
        async with aiogoogle as g:
            response = await g.as_user(batch_request,
                                       full_res=True)

        return response
    except Exception as e:
        print(f"...Error executing batch_request={batch_request} - {str(e)}")
        return batch_request