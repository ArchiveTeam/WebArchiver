import base64


def encode_message(message):
    message = [str(m) if type(m) != bytes else m for m in message]
    message = [m.encode('UTF-8') if type(m) != bytes else m for m in message]
    message = [base64.b64encode(m) for m in message]
    message = b';'.join(message)
    return message


def decode_message(message):
    message = message.split(b';')
    message = [base64.b64decode(m) for m in message]
    m_ = []
    for m in message:
        if len(m_) == 0 or m_[-1] != 'FILE':
            m_.append(m.decode('UTF-8'))
        else:
            del m_[-1]
            m_.append(m)
    return m_

