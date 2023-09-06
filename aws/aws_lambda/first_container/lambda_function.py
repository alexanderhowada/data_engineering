import json


def handler(event, context):
    event["new_key"] = "new_key"
    s = json.dumps(event)

    return {
        'statusCode': 200,
        'body': s
    }

if __name__ == '__main__':
    imp = {
        'asdf': 'this is an input',
        'fdsa': 'this is another input'
    }
    r = handler(imp, None)

    print(json.dumps(r, indent=4))

    