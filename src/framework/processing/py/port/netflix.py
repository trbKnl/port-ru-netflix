"""
DDP extract Netflix module
"""
from pathlib import Path
import logging
import zipfile
import json
from collections import Counter

import pandas as pd

import port.api.props as props
import port.unzipddp as unzipddp

from port.validate import (
    DDPCategory,
    Language,
    DDPFiletype,
    ValidateInput,
    StatusCode,
)

logger = logging.getLogger(__name__)

DDP_CATEGORIES = [
    DDPCategory(
        id="csv",
        ddp_filetype=DDPFiletype.CSV,
        language=Language.EN,
        known_files=["MyList.csv", "ViewingActivity.csv", "SearchHistory.csv", "IndicatedPreferences.csv", "PlaybackRelatedEvents.csv", "InteractiveTitles.csv", "Ratings.csv", "GamePlaySession.txt", "IpAddressesLogin.csv", "IpAddressesAccountCreation.txt", "IpAddressesStreaming.csv", "Additional Information.pdf", "MessagesSentByNetflix.csv", "SocialMediaConnections.txt", "AccountDetails.csv", "ProductCancellationSurvey.txt", "CSContact.csv", "ChatTranscripts.csv", "Cover sheet.pdf", "Devices.csv", "ParentalControlsRestrictedTitles.txt", "AvatarHistory.csv", "Profiles.csv", "Clickstream.csv", "BillingHistory.csv"]
    )
]

STATUS_CODES = [
    StatusCode(id=0, description="Valid zip", message="Valid zip"),
    StatusCode(id=1, description="Bad zipfile", message="Bad zipfile"),
]


def validate_zip(zfile: Path) -> ValidateInput:
    """
    Validates the input of an Instagram zipfile
    """

    validate = ValidateInput(STATUS_CODES, DDP_CATEGORIES)

    try:
        paths = []
        with zipfile.ZipFile(zfile, "r") as zf:
            for f in zf.namelist():
                p = Path(f)
                if p.suffix in (".txt", ".csv", ".pdf"):
                    logger.debug("Found: %s in zip", p.name)
                    paths.append(p.name)

        validate.set_status_code(0)
        validate.infer_ddp_category(paths)
    except zipfile.BadZipFile:
        validate.set_status_code(1)

    return validate


def extract_users_from_df(df: pd.DataFrame) -> list[str]:
    """
    Extracts all users from a netflix csv file 
    This function expects all users to be present in the first column
    of a pd.DataFrame
    """
    out = []
    try:
        out = df[df.columns[0]].unique().tolist()
        out.sort()
    except Exception as e:
        logger.error("Cannot extract users: %s", e)

    return out
    
def keep_user(df: pd.DataFrame, selected_user: str) -> pd.DataFrame:
    """
    Keep only the rows where the first column of df
    is equal to selected_user
    """
    try:
        df =  df.loc[df.iloc[:, 0] == selected_user].reset_index(drop=True)
    except Exception as e:  
        logger.info(e)

    return df

    
def netflix_to_df(netflix_zip: str, file_name: str, selected_user: str) -> pd.DataFrame:
    """
    netflix csv to df
    returns empty df in case of error
    """
    ratings_bytes = unzipddp.extract_file_from_zip(netflix_zip, file_name)
    df = unzipddp.read_csv_from_bytes_to_df(ratings_bytes)
    df = keep_user(df, selected_user)

    return df


def ratings_to_df(netflix_zip: str, selected_user: str)  -> pd.DataFrame:
    """
    Extract ratings from netflix zip to df
    Only keep the selected user
    """

    columns_to_keep = ["Title Name", "Thumbs Value", "Event Utc Ts"]
    columns_to_rename =  {
        "Title Name": "Titel",
        "Event Utc Ts": "Datum en tijd",
        "Thumbs Value": "Aantal duimpjes omhoog"
    }

    df = netflix_to_df(netflix_zip, "Ratings.csv", selected_user)

    # Extraction logic here
    try:
        if not df.empty:
            df = df[columns_to_keep]
            df = df.rename(columns=columns_to_rename)
    except Exception as e:
        logger.error("Data extraction error: %s", e)
        
    return df



def time_string_to_hours(time_str):
    try:
        # Split the time string into hours, minutes, and seconds
        hours, minutes, seconds = map(int, time_str.split(':'))

        # Convert each component to hours
        hours_in_seconds = hours * 3600
        minutes_in_seconds = minutes * 60

        # Sum up the converted values
        total_hours = (hours_in_seconds + minutes_in_seconds + seconds) / 3600
    except:
        return 0

    return round(total_hours, 3)


def viewing_activity_to_df(netflix_zip: str, selected_user: str)  -> pd.DataFrame:
    """
    Extract ViewingActivity from netflix zip to df
    Only keep the selected user
    """

    columns_to_keep = ["Start Time","Duration","Title","Supplemental Video Type"]
    columns_to_rename =  {
        "Start Time": "Start tijd",
        "Title": "Titel",
        "Supplemental Video Type": "Aanvullend informatie",
        "Duration": "Aantal uur gekeken"
    }

    df = netflix_to_df(netflix_zip, "ViewingActivity.csv", selected_user)
    remove_values = ["TEASER_TRAILER", "HOOK", "TRAILER", "CINEMAGRAPH"]

    # Extraction logic here
    try:
        if not df.empty:
            df = df[columns_to_keep]
            df = df[~df["Supplemental Video Type"].isin(remove_values)].reset_index(drop=True)
            df = df.rename(columns=columns_to_rename)

        df['Aantal uur gekeken'] = df['Aantal uur gekeken'].apply(time_string_to_hours)
        df = df.sort_values(by='Start tijd', ascending=True).reset_index(drop=True)
    except Exception as e:
        logger.error("Data extraction error: %s", e)
        
    return df


