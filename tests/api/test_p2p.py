import copy
import json

import pytest
from configmanager import Config

P2P_PUB_URI = "/api/v0/p2p/pubsub/pub"

MESSAGE_DICT = {
    "chain": "NULS2",
    "item_hash": "4bbcfe7c4775492c2e602d322d68f558891468927b5e0d6cb89ff880134f323e",
    "sender": "NULSd6Hgbhr42Dm5nEgf6foEUT5bgwHesZQJB",
    "type": "STORE",
    "channel": "MYALEPH",
    "item_content": '{"address":"NULSd6Hgbhr42Dm5nEgf6foEUT5bgwHesZQJB","item_type":"ipfs","item_hash":"QmUDS8mpQmpPyptyUEedHxHMkxo7ueRRiAvrpgvJMpjXwW","time":1577325086.513}',
    "item_type": "inline",
    "signature": "G7/xlWoMjjOr1NBN4SiZ8USYYVM9Q3JHXChR9hPw9/YSItfAplshWysqYDkvmBZiwbICG0IVB3ilMPJ/ZVgPNlk=",
    "time": 1608297193.717,
}


@pytest.mark.asyncio
async def test_pubsub_pub_valid_message(ccn_api_client, mock_config: Config):
    message_topic = mock_config.aleph.queue_topic.value

    response = await ccn_api_client.post(
        P2P_PUB_URI, json={"topic": message_topic, "data": json.dumps(MESSAGE_DICT)}
    )
    assert response.status == 200, await response.text()
    response_json = await response.json()

    assert response_json["status"] == "success"


@pytest.mark.asyncio
async def test_pubsub_pub_errors(ccn_api_client, mock_config: Config):
    # Invalid topic
    serialized_message_dict = json.dumps(MESSAGE_DICT)
    response = await ccn_api_client.post(
        P2P_PUB_URI, json={"topic": "random-topic", "data": serialized_message_dict}
    )
    assert response.status == 403, await response.text()

    message_topic = mock_config.aleph.queue_topic.value

    # Do not serialize the message
    response = await ccn_api_client.post(
        P2P_PUB_URI, json={"topic": message_topic, "data": MESSAGE_DICT}
    )
    assert response.status == 422, await response.text()

    # Invalid JSON
    response = await ccn_api_client.post(
        P2P_PUB_URI, json={"topic": message_topic, "data": serialized_message_dict[:-2]}
    )
    assert response.status == 422, await response.text()

    # Invalid message
    message_dict = copy.deepcopy(MESSAGE_DICT)
    del message_dict["item_content"]

    response = await ccn_api_client.post(
        P2P_PUB_URI, json={"topic": message_topic, "data": json.dumps(message_dict)}
    )
    assert response.status == 422, await response.text()
