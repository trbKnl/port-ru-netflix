import logging
import json
import io

import pandas as pd

from port.api.commands import (CommandSystemDonate, CommandSystemExit, CommandUIRender)
import port.api.props as props
import port.unzipddp as unzipddp
import port.netflix as netflix


LOG_STREAM = io.StringIO()

logging.basicConfig(
    stream=LOG_STREAM,
    level=logging.INFO,
    format="%(asctime)s --- %(name)s --- %(levelname)s --- %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S%z",
)

LOGGER = logging.getLogger("script")

TABLE_TITLES = {
    "netflix_ratings": props.Translatable(
        {
            "en": "Ratings you gave according to Netlix:",
            "nl": "Jouw beoordelingen volgens Netflix:",
        }
    ),
}

# Questionnaire questions
UNDERSTANDING = props.Translatable({
    "en": "How would you describe the information that you shared with researchers fro mthe Radboud University?",
    "nl": "Hoe zou u de gegevens omschrijven die u heeft gedeeld met onderzoekers van de Radboud Universiteit?"
})

INDENTIFY_CONSUMPTION = props.Translatable({"en": "In case you looked at the data presented on this page, did you recognise your Netflix watching patterns?", "nl": "Als u naar uw data gekeken hebt, in hoeverre herkent u uw eigen kijkgedrag?"})
IDENTIFY_CONSUMPTION_CHOICES = [
    props.Translatable({"en": "I recognized my Netflix watching patterns", "nl": "Ik herkende mijn Netflix kijkgedrag"}),
    props.Translatable({"en": "I recognized my Netflix watching patterns and patters of those I share my account with", "nl": "Ik herkende mijn eigen Netflix kijkgedrag en die van anderen met wie ik mijn account deel"}),
    props.Translatable({"en": "I recognized mostly the watching patterns of those I share my account with", "nl": "Ik herkende vooral het kijkgedrag van anderen met wie ik mijn account deel"}),
    props.Translatable({"en": "I did not look at my data ", "nl": "Ik heb niet naar mijn gegevens gekeken"}),
    props.Translatable({"en": "Other", "nl": "Anders"})
]

ENJOYMENT = props.Translatable({"en": "In case you looked at the data presented on this page, how interesting did you find looking at your data?", "nl": "Als u naar uw data hebt gekeken, hoe interessant vond u het om daar naar te kijken?"})
ENJOYMENT_CHOICES = [
    props.Translatable({"en": "not at all interesting", "nl": "Helemaal niet interessant"}),
    props.Translatable({"en": "somewhat uninteresting", "nl": "Een beetje oninteressant"}),
    props.Translatable({"en": "neither interesting nor uninteresting", "nl": "Niet interessant, niet oninteressant"}),
    props.Translatable({"en": "somewhat interesting", "nl": "Een beetje interessant"}),
    props.Translatable({"en": "very interesting", "nl": "Erg interessant"})
]

ADDITIONAL_COMMENTS = props.Translatable({
    "en": "Do you have any additional comments about the donation? Please add them here.",
    "nl": "Heeft u nog andere opmerkingen? Laat die hier achter."
})

#Not donate questions
NO_DONATION_REASONS = props.Translatable({
    "en": "What is/are the reason(s) that you decided not to donate your data?",
    "nl": "Wat is de reden dat u er voor gekozen hebt uw data niet te delen?"
})

# Headers
SUBMIT_FILE_HEADER = props.Translatable({
    "en": "Select your Netflix file", 
    "nl": "Selecteer uw Netflix bestand"
})

REVIEW_DATA_HEADER = props.Translatable({
    "en": "Your Netflix data", 
    "nl": "Uw Netflix gegevens"
})

RETRY_HEADER = props.Translatable({
    "en": "Try again", 
    "nl": "Probeer opnieuw"
})


def process(session_id):
    LOGGER.info("Starting the donation flow")
    yield donate_logs(f"{session_id}-tracking")

    platform_name = "Netflix"
    table_list = None

    while True:
        LOGGER.info("Prompt for file for %s", platform_name)
        yield donate_logs(f"{session_id}-tracking")

        promptFile = prompt_file("application/zip, text/plain", platform_name)
        file_result = yield render_page(SUBMIT_FILE_HEADER, promptFile)
        selected_user = ""

        if file_result.__type__ == "PayloadString":
            validation = netflix.validate_zip(file_result.value)

            # Flow logic
            # Happy flow: Valid DDP, user was set selected
            # Retry flow 1: No user was selected, cause could be for multiple reasons see code
            # Retry flow 2: No valid Netflix DDP was found
            # Retry flows are separated for clarity and you can provide different messages to the user

            if validation.ddp_category is not None:
                LOGGER.info("Payload for %s", platform_name)
                yield donate_logs(f"{session_id}-tracking")

                # Extract the user
                users = extract_users(file_result.value)

                if len(users) == 1:
                    selected_user = users[0]
                    extraction_result = extract_netflix(file_result.value, selected_user)
                    table_list = extraction_result
                elif len(users) > 1:
                    selection = yield prompt_radio_menu_select_username(users)
                    if selection.__type__ == "PayloadString":
                        selected_user = selection.value
                        extraction_result = extract_netflix(file_result.value, selected_user)
                        table_list = extraction_result
                    else:
                        LOGGER.info("User skipped during user selection")
                        pass
                else:
                    LOGGER.info("No users could be found in DDP")
                    pass

            # Enter retry flow, reason: if DDP was not a Netflix DDP
            if validation.ddp_category is None:
                LOGGER.info("Not a valid %s zip; No payload; prompt retry_confirmation", platform_name)
                yield donate_logs(f"{session_id}-tracking")
                retry_result = yield render_page(RETRY_HEADER, retry_confirmation(platform_name))

                if retry_result.__type__ == "PayloadTrue":
                    continue
                else:
                    LOGGER.info("Skipped during retry ending flow")
                    yield donate_logs(f"{session_id}-tracking")
                    yield donate_status(f"{session_id}-SKIP-RETRY-FLOW", "SKIP_RETRY_FLOW")
                    break

            # Enter retry flow, reason: valid DDP but no users could be extracted
            if selected_user == "":
                LOGGER.info("Selected user is empty after selection, enter retry flow")
                yield donate_logs(f"{session_id}-tracking")
                retry_result = yield render_page(RETRY_HEADER, retry_confirmation(platform_name))

                if retry_result.__type__ == "PayloadTrue":
                    continue
                else:
                    LOGGER.info("Skipped during retry ending flow")
                    yield donate_logs(f"{session_id}-tracking")
                    yield donate_status(f"{session_id}-SKIP-RETRY-FLOW", "SKIP_RETRY_FLOW")
                    break

        else:
            LOGGER.info("Skipped at file selection ending flow")
            yield donate_logs(f"{session_id}-tracking")
            yield donate_status(f"{session_id}-SKIP-FILE-SELECTION", "SKIP_FILE_SELECTION")
            break


        if table_list is not None:
            LOGGER.info("Prompt consent; %s", platform_name)
            yield donate_logs(f"{session_id}-tracking")
            prompt = create_consent_form(table_list)
            consent_result = yield render_page(REVIEW_DATA_HEADER, prompt)

            # Data was donated
            if consent_result.__type__ == "PayloadJSON":
                LOGGER.info("Data donated; %s", platform_name)
                yield donate(f"{session_id}-{platform_name}", consent_result.value)
                yield donate_logs(f"{session_id}-tracking")
                yield donate_status(f"{session_id}-DONATED", "DONATED")

                # render happy questionnaire
                render_questionnaire_results = yield render_questionnaire()

                if render_questionnaire_results.__type__ == "PayloadJSON":
                    yield donate(f"{session_id}-questionnaire-donation", render_questionnaire_results.value)
                else:
                    LOGGER.info("Skipped questionnaire: %s", platform_name)
                    yield donate_logs(f"{session_id}-tracking")

            # Data was not donated
            else:
                LOGGER.info("Skipped ater reviewing consent: %s", platform_name)
                yield donate_logs(f"{session_id}-tracking")
                yield donate_status(f"{session_id}-SKIP-REVIEW-CONSENT", "SKIP_REVIEW_CONSENT")


                # render sad questionnaire
                render_questionnaire_results = yield render_questionnaire_no_donation()
                if render_questionnaire_results.__type__ == "PayloadJSON":
                    yield donate(f"{session_id}-questionnaire-no-donation", render_questionnaire_results.value)
                else:
                    LOGGER.info("Skipped questionnaire: %s", platform_name)
                    yield donate_logs(f"{session_id}-tracking")

            break

    yield exit(0, "Success")
    yield render_end_page()


##################################################################

def create_consent_form(table_list: list[props.PropsUIPromptConsentFormTable]) -> props.PropsUIPromptConsentForm:
    """
    Assembles all donated data in consent form to be displayed
    """
    desc = props.Translatable({
        "en": "Determine whether you want to share the data below. Review the data carefully and adjust if necessary. Only the data that is visualized will be shared. Your contribution will help the previously described research. Thank you in advance.",
        "nl": "Bepaal of u de onderstaande gegevens wilt delen. Bekijk de gegevens zorgvuldig en pas zo nodig aan. Alleen de gegevens die zijn gevisualiseerd, worden gedeeld. Met uw bijdrage helpt u het eerder beschreven onderzoek. Alvast hartelijk dank.",
    })
    return props.PropsUIPromptConsentForm(table_list, description=desc, meta_tables=[])


def return_empty_result_set():
    result = {}

    df = pd.DataFrame(["No data found"], columns=["No data found"])
    result["empty"] = {"data": df, "title": TABLE_TITLES["empty_result_set"]}

    return result


def donate_logs(key):
    log_string = LOG_STREAM.getvalue()  # read the log stream
    if log_string:
        log_data = log_string.split("\n")
    else:
        log_data = ["no logs"]

    return donate(key, json.dumps(log_data))


def donate_status(filename: str, message: str):
    return donate(filename, json.dumps({"status": message}))


def prompt_radio_menu_select_username(users):
    """
    Prompt selection menu to select which user you are
    """

    title = props.Translatable({ "en": "Select your Netflix profile name", "nl": "Kies jouw Netflix profielnaam" })
    description = props.Translatable({ "en": "", "nl": "" })
    header = props.PropsUIHeader(props.Translatable({"en": "", "nl": ""}))

    radio_items = [{"id": i, "value": username} for i, username in enumerate(users)]
    body = props.PropsUIPromptRadioInput(title, description, radio_items)
    footer = props.PropsUIFooter()

    page = props.PropsUIPageDonation("Netflix", header, body, footer)

    return CommandUIRender(page)


##################################################################
# Extraction function

# The A conditional group gets the visualizations 
def extract_netflix(netflix_zip: str, selected_user: str) -> list[props.PropsUIPromptConsentFormTable]:
    """
    Main data extraction function
    Assemble all extraction logic here, results are stored in a dict
    """

    tables_to_render = []
    
    # Extract the ratings
    ###################################################################

    df = netflix.ratings_to_df(netflix_zip, selected_user)
    if not df.empty:
        wordcloud = {
            "title": {"en": "Titles rated by thumbs value", "nl": "Gekeken titles, grootte is gebasseerd op het aantal duimpjes omhoog"},
            "type": "wordcloud",
            "textColumn": "Titel",
            "valueColumn": "Aantal duimpjes omhoog",
        }
        table_title = props.Translatable({"en": "Your ratings on Netflix", "nl": "Uw beoordelingen op Netflix"})
        table_description = props.Translatable({
            "en": "Click 'Show Table' to view these ratings per row.", 
            "nl": "Klik op ‘Tabel tonen’ om deze beoordelingen per rij te bekijken."
        })
        table = props.PropsUIPromptConsentFormTable("netflix_rating", table_title, df, table_description, [wordcloud])
        tables_to_render.append(table)


    df = netflix.viewing_activity_to_df(netflix_zip, selected_user)
    if not df.empty:

        hours_logged_in = {
            "title": {"en": "Total hours watched per month of the year", "nl": "Totaal aantal uren gekeken per maand van het jaar"},
            "type": "area",
            "group": {
                "column": "Start tijd",
                "dateFormat": "month",
                "label": "Month"
            },
            "values": [{
                "column": "Aantal uur gekeken",
                "aggregate": "sum",
            }]
        }

        at_what_time = {
            "title": {"en": "Total hours watch by hour of the day", "nl": "Totaal aantal uur gekeken op uur van de dag"},
            "type": "bar",
            "group": {
                "column": "Start tijd",
                "dateFormat": "hour_cycle"
            },
            "values": [{
                "column": "Aantal uur gekeken",
                "aggregate": "sum",
            }]
        }


        table_title = props.Translatable({"en": "What you watched", "nl": "Wanneer kijkt u Netflix"})
        table_description = props.Translatable({
            "en": "This table shows what titles you watched when and for how long.", 
            "nl": "Klik op ‘Tabel tonen’ om voor elke keer dat u iets op Netflix heeft gekeken te zien welke serie of film dit was, wanneer u dit heeft gekeken, hoe lang u het heeft gekeken."
        })
        table = props.PropsUIPromptConsentFormTable("netflix_viewings", table_title, df, table_description, [hours_logged_in, at_what_time])
        tables_to_render.append(table)

    return tables_to_render




def extract_users(netflix_zip):
    """
    Reads viewing activity and extracts users from the first column
    returns list[str]
    """
    b = unzipddp.extract_file_from_zip(netflix_zip, "ViewingActivity.csv")
    df = unzipddp.read_csv_from_bytes_to_df(b)
    users = netflix.extract_users_from_df(df)
    return users


##########################################################################################
# Questionnaires

def render_questionnaire():
    questions = [
        props.PropsUIQuestionMultipleChoice(question=INDENTIFY_CONSUMPTION, id=2, choices=IDENTIFY_CONSUMPTION_CHOICES),
        props.PropsUIQuestionMultipleChoice(question=ENJOYMENT, id=3, choices=ENJOYMENT_CHOICES),
        props.PropsUIQuestionOpen(question=ADDITIONAL_COMMENTS, id=4),
    ]

    description = props.Translatable({"en": "Below you can find a couple of questions about the data donation process", "nl": "Hieronder vindt u een paar vragen over het proces van het data delen"})
    header = props.PropsUIHeader(props.Translatable({"en": "Questionnaire", "nl": "Vragenlijst"}))
    body = props.PropsUIPromptQuestionnaire(questions=questions, description=description)
    footer = props.PropsUIFooter()

    page = props.PropsUIPageDonation("ASD", header, body, footer)
    return CommandUIRender(page)


def render_questionnaire_no_donation():
    questions = [
        props.PropsUIQuestionMultipleChoice(question=INDENTIFY_CONSUMPTION, id=2, choices=IDENTIFY_CONSUMPTION_CHOICES),
        props.PropsUIQuestionMultipleChoice(question=ENJOYMENT, id=3, choices=ENJOYMENT_CHOICES),
        props.PropsUIQuestionOpen(question=NO_DONATION_REASONS, id=5),
        props.PropsUIQuestionOpen(question=ADDITIONAL_COMMENTS, id=4),
    ]

    description = props.Translatable({"en": "Below you can find a couple of questions about the data donation process", "nl": "Hieronder vind u een paar vragen over het data donatie process"})
    header = props.PropsUIHeader(props.Translatable({"en": "Questionnaire", "nl": "Vragenlijst"}))
    body = props.PropsUIPromptQuestionnaire(questions=questions, description=description)
    footer = props.PropsUIFooter()

    page = props.PropsUIPageDonation("ASD", header, body, footer)
    return CommandUIRender(page)


def render_end_page():
    page = props.PropsUIPageEnd()
    return CommandUIRender(page)


def render_page(header_text, body):
    header = props.PropsUIHeader(header_text)

    footer = props.PropsUIFooter()
    platform = "Netflix"
    page = props.PropsUIPageDonation(platform, header, body, footer)
    return CommandUIRender(page)


def retry_confirmation(platform):
    text = props.Translatable(
        {
            "en": f"Unfortunately, we could not process your {platform} file. If you are sure that you selected the correct file, press Continue. To select a different file, press Try again.",
            "nl": f"Helaas, kunnen we uw {platform} bestand niet verwerken. Weet u zeker dat u het juiste bestand heeft gekozen? Ga dan verder. Probeer opnieuw als u een ander bestand wilt kiezen."
        }
    )
    ok = props.Translatable({"en": "Try again", "nl": "Probeer opnieuw"})
    cancel = props.Translatable({"en": "Continue", "nl": "Verder"})
    return props.PropsUIPromptConfirm(text, ok, cancel)


def prompt_file(extensions, platform):
    description = props.Translatable(
        {
            "en": f"Please follow the download instructions and choose the file that you stored on your device.",
            "nl": f"Volg de download instructies en kies het bestand dat u opgeslagen heeft op uw apparaat."
        }
    )
    return props.PropsUIPromptFileInput(description, extensions)


def donate(key, json_string):
    return CommandSystemDonate(key, json_string)

def exit(code, info):
    return CommandSystemExit(code, info)
