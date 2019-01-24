"""Run a GraphQL Query on the GitHub API Endpoint.

"""

import requests
import json
import os
from loguru import logger
from pprint import pprint
from dotenv import load_dotenv

# from github_data import Repository, StarGazer, GitHubUser

load_dotenv()
_log_file_name = __file__.split("/")[-1].split(".")[0]
logger.add(f"logs/{_log_file_name}.log", rotation="1 day")
logger.add(f"logs/success.log", rotation="1 day", level="SUCCESS")

# The Query Template is generated via Insomina
QUERY_TEMPLATE = '{"query":"\\nquery { \\n\\trepository(owner:\\"rep_owner\\", name:\\"rep_name\\") {\\n    stargazers(stargazer_limitation_str, orderBy:{field: STARRED_AT, direction: ASC}) {\\n      totalCount\\n      edges {\\n        starredAt\\n        node {\\n          name\\n          createdAt\\n          followers {\\n            totalCount\\n          }\\n          starredRepositories {\\n            totalCount\\n          }\\n          url\\n        }\\n        cursor\\n      }\\n      pageInfo {\\n        endCursor\\n      }\\n    }\\n    collaborators {\\n      totalCount\\n    }\\n    watchers(last:5) {\\n      totalCount\\n\\n    }\\n    createdAt\\n    isFork\\n    forkCount\\n    nameWithOwner\\n    primaryLanguage {\\n      name\\n      id\\n    }\\n    languages(first:40) {\\n      totalCount\\n      nodes {\\n        name\\n        id\\n      }\\n    }\\n    \\n  }\\n}"}'
REPOSITORY_INFO_QUERY = '{"query":"query { \\n\\trepository(owner:\\"rep_owner\\", name:\\"rep_name\\") {\\n    stargazers {\\n      totalCount\\n    },\\n    watchers {\\n      totalCount\\n    },\\n    owner {\\n      id\\n    },\\n    url,\\n    createdAt,\\n    isFork,\\n    forkCount,\\n    nameWithOwner,\\n    primaryLanguage {\\n      name\\n    }\\n    id\\n    languages(first:5, orderBy:{field:SIZE, direction:DESC}) {\\n      totalCount,\\n      nodes {\\n        name\\n      }\\n    }\\n  }\\n}"}'
STARGAZER_INFO_QUERY_INITIAL = '{"query":"query { \\n\\trepository(owner:\\"rep_owner\\", name:\\"rep_name\\") {\\n    stargazers(first:100, orderBy:{field: STARRED_AT, direction: ASC}) {\\n      edges {\\n        starredAt\\n        node {\\n          name,\\n          id,\\n          url,\\n        },\\n      },\\n      pageInfo {\\n        endCursor\\n      hasNextPage\\n      }\\n    },\\n  }\\n}"}'
STARGAZER_INFO_QUERY_CURSOR = '{"query":"query { \\n\\trepository(owner:\\"rep_owner\\", name:\\"rep_name\\") {\\n    stargazers(first:100, after:\\"sg_eo_page_cursor\\", orderBy:{field: STARRED_AT, direction: ASC}) {\\n      edges {\\n        starredAt\\n        node {\\n          name,\\n          id,\\n          url,\\n        },\\n      },\\n      pageInfo {\\n        endCursor\\n      hasNextPage\\n      }\\n    },\\n  }\\n}"}'

GITHUB_API_ENDPOINT = os.getenv(
    "GITHUB_API_URL", default="https://api.github.com/graphql"
)
GITHUB_USERNAME = os.getenv("GITHUB_USERNAME")
GITHUB_API_TOKEN = os.getenv("GITHUB_API_ACCESS_TOKEN")

"""
SG_LIMITATION_STR_TEMPLATE Options
after: String

Returns the elements in the list that come after the specified cursor.
before: String

Returns the elements in the list that come before the specified cursor.
first: Int

Returns the first n elements from the list.
last: Int

Returns the last n elements from the list.

"""
SG_LIMITATION_STR_TEMPLATE = "last:10"


def run_gql_query(endpoint_url, query, auth):
    logger.info(f"Run GraphQL Query against: {endpoint_url}.")
    response = requests.post(url=endpoint_url, data=query, auth=auth)
    logger.info(f"Got a response code of: {response.status_code}.")

    return response


def prepare_gql_query(query, **data):
    """Modify the gql query to contain the correct data.

    This will be necessary for paginated pages, where we need to modify the next query based on a returned cursor.

    data: collects all keywords that are necessary to populate the query.

    return: The populated query as string.
    """

    logger.info(f"Preparing GQL query with data: {data}")
    # query.format(**data)

    for key, value in data.items():
        query = query.replace(key, value)

    return query


def response2json(response):
    logger.info(f"Converting HTTP response content to json.")
    return json.loads(response.content)


def clean_gql_query_response(response):
    """Call all the clean up functions to get usable data.
    """

    data_json = response2json(response)
    if data_json is None:
        logger.warning("Encountered a NoneType for response: {response}.")
    return data_json


def flatten_response_json(data):
    # flatten
    repository = data["data"]["repository"]
    if repository is None:
        logger.warning(f"Encountered a NoneType in flatten_response_json.data: {data}")

    return repository


def get_raw_repository_info(**data):
    """Return the basic repo info.
    """
    logger.info(f"Get raw repository info for {data}.")
    query = prepare_gql_query(query=REPOSITORY_INFO_QUERY, **data)
    response = run_gql_query(
        GITHUB_API_ENDPOINT, query, auth=(GITHUB_USERNAME, GITHUB_API_TOKEN)
    )
    repo_data = clean_gql_query_response(response)
    repo_data = flatten_response_json(repo_data)

    return repo_data


def get_raw_stargazer_info(has_next_page=False, is_initial=True, **data):
    if is_initial:
        logger.info("Get raw stargazer info - Initial")
        query = prepare_gql_query(query=STARGAZER_INFO_QUERY_INITIAL, **data)
        is_initial = False
    else:
        logger.info("Get raw stargazer info - Repeat")
        query = prepare_gql_query(query=STARGAZER_INFO_QUERY_CURSOR, **data)

    response = run_gql_query(
        GITHUB_API_ENDPOINT, query, auth=(GITHUB_USERNAME, GITHUB_API_TOKEN)
    )

    # sg (stargazer)
    sg_data = clean_gql_query_response(response)
    sg_data = flatten_response_json(sg_data)["stargazers"]
    stargazers = sg_data["edges"]

    # This works only if data was parsed by json.load
    if sg_data["pageInfo"]["hasNextPage"]:
        data["sg_eo_page_cursor"] = sg_data["pageInfo"]["endCursor"]
        has_next_page = True
    else:
        has_next_page = False

    if has_next_page:
        stargazers += get_raw_stargazer_info(
            has_next_page=has_next_page, is_initial=False, **data
        )

    return stargazers


def flatten_data(data):
    return data


# # DEPRECATED
# def get_repository_data(rep_owner, rep_name):
#     """Get all data for a single repository.

#     Follow all cursors until everything is fetched.

#     rep_owner: Name of the owner of the repository
#     rep_name: Name of the repository

#     Example: github.com/JimFawkes/luther/
#     -> rep_owner = JimFawkes
#     -> rep_name = luther

#     Return a dictionary-like object that holds all relevant data for a single respository.
#     """

#     # Will need to loop over the two functions prepare_gql_query and run_gql_query
#     query = prepare_gql_query(
#         query=QUERY_TEMPLATE, rep_owner=rep_owner, rep_name=rep_name
#     )
#     response = run_gql_query(
#         endpoint_url=GITHUB_API_ENDPOINT,
#         query=query,
#         auth=(GITHUB_USERNAME, GITHUB_API_TOKEN),
#     )

#     data = clean_gql_query_response(response)

#     return data
