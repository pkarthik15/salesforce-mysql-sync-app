from salesforce import get_auth_token
from object_sync import update_sync
from config import Configurations, get_date_time_n_mins_ago_utc




date_time_15_mins_ago = get_date_time_n_mins_ago_utc(30)
# lead_query = "SELECT Id, Center__c, ConvertedAccountId, ConvertedContactId, ConvertedOpportunityId, CreatedById, Email, HasOptedOutOfEmail, IndividualId, Initial_Consultation_Appointment__c, Initial_Inquiry__c, Initial_Lead_Owner__c, LastModifiedById, MasterRecordId, Most_Recent_Consultation_Appointment__c,  Most_Recent_Inquiry__c, OwnerId, RecordTypeId, Referral_Source__c, Status, On_Hold__c, Name, FirstName, LastModifiedDate, Phone, Center_City__c, Center_Friendly_Name__c, Center_State__c, CreatedDate, DateOfBirth__c, Interested_Services_TEXT__c, Is_Test_Record__c, Last_Completed_Activity_Type__c, Receive_Promo_Emails__c, Center_Country__c, Center_Type__c, Most_Recent_Inquiry_Interested_services__c, Lead__c, Most_Recent_Consultation_Date__c, IsConverted,  IO_IsBanned__c, Guest_ID_TEXT__c, Lead_Owner_Full_Name__c, Receive_Promotional_Text__c, HasOptedOutOfText__c, Center_Zip__c, Center_Phone__c, Receiving_Transactional_Text__c, Center_Time_Zone__c FROM Lead WHERE Center_Country__c = 'United States' AND IsConverted = false AND ((Most_Recent_Inquiry_Date__c >= N_DAYS_AGO:550 AND HasOptedOutOfEmail = false) OR Receiving_Promotional_Text__c = true)"

lead_query = "Select Id, ConvertedContactId, HasOptedOutOfEmail, Receive_Transactional_Text__c, Receiving_Promotional_Text__c, Most_Recent_Inquiry__c, Most_Recent_Consultation_Appointment__c, Most_Recent_Inquiry_Interested_Services__c, Guest_ID_TEXT__c, Initial_Lead_Owner__c, OwnerId, CreatedDate, Inquiry_Campaign_Day__c, IsConverted, Status, Initial_Inquiry_Source__c, Center__c, Center_Name__c, LastModifiedDate From Lead  WHERE Center_Country__c = 'United States' AND IsConverted = false AND ((Most_Recent_Inquiry_Date__c >= N_DAYS_AGO:550 AND HasOptedOutOfEmail = false) OR Receiving_Promotional_Text__c = true)"

 
print("Sync started")
session, url = get_auth_token(Configurations.SALESFORCE_ENVIRONMENT, Configurations.SALESFORCE_USERNAME, Configurations.SALESFORCE_PASSWORD, Configurations.SALESFORCE_CLIENT_ID, Configurations.SALESFORCE_CLIENT_SECRET);

if (session and url):
    print("User authenticated successfully")
    update_sync(session, url, lead_query, "sf_lead")

print("Sync completed")

