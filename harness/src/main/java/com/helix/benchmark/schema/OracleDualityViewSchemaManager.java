package com.helix.benchmark.schema;

import com.helix.benchmark.config.SchemaModel;

import java.util.ArrayList;
import java.util.List;

public class OracleDualityViewSchemaManager implements SchemaManager {

    @Override
    public List<String> getCollectionNames(SchemaModel model) {
        return getViewNames();
    }

    public List<String> getViewNames() {
        List<String> views = new ArrayList<>();
        views.add("dv_book_role_investor");
        views.add("dv_book_role_group");
        views.add("dv_account");
        views.add("dv_advisor");
        return views;
    }

    public List<String> getCreateViewStatements() {
        List<String> ddl = new ArrayList<>();

        ddl.add("""
                CREATE OR REPLACE JSON RELATIONAL DUALITY VIEW dv_book_role_investor AS
                  rel_book_role_investor
                  {
                    _id                         : id
                    partyRoleId                 : party_role_id
                    investorType                : investor_type
                    investorFullName            : investor_full_name
                    viewableFlag                : viewable_flag
                    viewableSource              : viewable_source
                    entDataOwnerPartyRoleId     : ent_data_owner_party_role_id
                    partyId                     : party_id
                    conversionInProgress        : conversion_in_progress
                    dataOwnerPartyRoleId        : data_owner_party_role_id
                    investorId                  : investor_id_field
                    entity                      : entity
                    totalMarketValue            : total_market_value
                    totalAccounts               : total_accounts
                    totalViewableAccountsMarketValue : total_viewable_accts_market_value
                    totalViewableAccountCount   : total_viewable_account_count
                    ssnTin                      : ssn_tin
                    finInstId                   : fin_inst_id
                    investorLastName            : investor_last_name
                    investorFirstName           : investor_first_name
                    investorMiddleName          : investor_middle_name
                    investorpartyRoleId         : investor_party_role_id
                    investorCity                : investor_city
                    investorState               : investor_state
                    investorZipCode             : investor_zip_code
                    investorBirthdate           : investor_birthdate
                    clientAccess                : client_access
                    trustFlag                   : trust_flag
                    updateTmst                  : update_tmst
                    setupTmst                   : setup_tmst
                    ETLUpdateTS                 : etl_update_ts
                    pxClientId                  : px_client_id
                    pxClientName                : px_client_name
                    pxClientRefId               : px_client_ref_id
                    advisors                    : rel_bri_advisors
                    [
                      {
                        advisorId               : advisor_id
                        viewableMarketValue     : viewable_mv
                        noOfViewableAccts       : no_of_viewable_accts
                        advisorName             : advisor_name
                        advisorTaxId            : advisor_tax_id
                        advisorFinInstId        : advisor_fin_inst_id
                        lastName                : last_name
                        firstName               : first_name
                        middleName              : middle_name
                        state                   : state
                        city                    : city
                        zipCode                 : zip_code
                        country                 : country
                        businessPhone           : business_phone
                        bookType                : book_type
                        marketValue             : market_value
                        noOfAccts               : no_of_accts
                        status                  : status
                        isPrimary               : is_primary
                        email                   : email
                      }
                    ]
                    advisoryContexts            : rel_bri_advisory_ctx
                    [
                      {
                        advisoryContext         : advisory_context
                      }
                    ]
                    bookRoles                   : rel_bri_adv_book_roles
                    [
                      {
                        advisorId              : advisor_id
                        bookRole               : book_role
                      }
                    ]
                    investorHierarchy           : rel_bri_investor_hierarchy
                    [
                      {
                        partyNodePathNm        : party_node_path_nm
                        partyNodePathValue     : party_node_path_value
                      }
                    ]
                    personaNm                   : rel_bri_persona_nm
                    [
                      {
                        personaNm              : persona_nm
                      }
                    ]
                    synonyms                    : rel_bri_synonyms
                    [
                      {
                        partySynonymTypeCd     : synonym_type_cd
                        partySynonymStr        : synonym_str
                      }
                    ]
                  }""");

        ddl.add("""
                CREATE OR REPLACE JSON RELATIONAL DUALITY VIEW dv_book_role_group AS
                  rel_book_role_group
                  {
                    _id                              : id
                    dataOwnerPartyRoleId             : data_owner_party_role_id
                    visibleFlag                      : visible_flag
                    totalViewableAccountsMarketValue : total_viewable_accts_market_value
                    investorWriId                    : investor_wri_id
                    etlSourceGroup                   : etl_source_group
                    finInstId                        : fin_inst_id
                    entity                           : entity
                    accountCount                     : account_count
                    totalMarketValue                 : total_market_value
                    totalViewableAccountCount        : total_viewable_account_count
                    accountGroupName                 : account_group_name
                    accountGroupId                   : account_group_id
                    accountGroupType                 : account_group_type
                    portfolioType                    : portfolio_type
                    ETLUpdateTS                      : etl_update_ts
                    pxClientId                       : px_client_id
                    pxClientName                     : px_client_name
                    pxClientRefId                    : px_client_ref_id
                    advisoryContexts                 : rel_brg_advisory_ctx
                    [
                      {
                        advisoryContext              : advisory_context
                      }
                    ]
                    personaNms                       : rel_brg_persona_nm
                    [
                      {
                        personaNm                    : persona_nm
                      }
                    ]
                    partyRoleIds                     : rel_brg_party_role_ids
                    [
                      {
                        partyRoleId                  : party_role_id
                      }
                    ]
                    advisors                         : rel_brg_advisors
                    [
                      {
                        advisorId                    : advisor_id
                        advisorTaxId                 : advisor_tax_id
                        finInstId                    : fin_inst_id
                        firstName                    : first_name
                        middleName                   : middle_name
                        lastName                     : last_name
                        advisorName                  : advisor_name
                        bookType                     : book_type
                        totalViewableAccountsMarketValue : total_viewable_accts_market_value
                        totalViewableAccountCount    : total_viewable_account_count
                        noOfViewableAccts            : no_of_viewable_accts
                        viewableMarketValue          : viewable_market_value
                        status                       : status
                      }
                    ]
                    advisorBookRoles                 : rel_brg_adv_book_roles
                    [
                      {
                        advisorId                    : advisor_id
                        bookRole                     : book_role
                      }
                    ]
                    advisorInvestors                 : rel_brg_adv_investors
                    [
                      {
                        advisorId                    : advisor_id
                        investorId                   : investor_id
                      }
                    ]
                    advisorHierarchy                 : rel_brg_hierarchy
                    [
                      {
                        partyNodePathValue           : party_node_path_value
                      }
                    ]
                  }""");

        ddl.add("""
                CREATE OR REPLACE JSON RELATIONAL DUALITY VIEW dv_account AS
                  rel_account
                  {
                    _id                : id
                    viewableSource     : viewable_source
                    accountid          : account_id_field
                    ssnTin             : ssn_tin
                    finInstId          : fin_inst_id
                    clientName         : client_name
                    clientId           : client_id
                    finInstName        : fin_inst_name
                    accountType        : account_type
                    acctName           : acct_name
                    viewable           : viewable
                    setupTmst          : setup_tmst
                    updateTmst         : update_tmst
                    acctTitle          : acct_title
                    category           : category
                    ETLUpdateTS        : etl_update_ts
                    pxClientId         : px_client_id
                    pxClientName       : px_client_name
                    pxClientRefId      : px_client_ref_id
                    partyRoleIds       : rel_acct_party_role_ids
                    [
                      {
                        partyRoleId    : party_role_id
                      }
                    ]
                    holdings           : rel_acct_holdings
                    [
                      {
                        fundTicker     : fund_ticker
                        fundName       : fund_name
                        fundId         : fund_id
                        mgtName        : mgt_name
                        dividendRate   : dividend_rate
                      }
                    ]
                    advisors           : rel_acct_advisors
                    [
                      {
                        advisorId          : advisor_id
                        viewableMarketValue : viewable_mv
                        noOfViewableAccts  : no_of_viewable_accts
                        advisorName        : advisor_name
                        advisorTaxId       : advisor_tax_id
                        advisorFinInstId   : advisor_fin_inst_id
                        lastName           : last_name
                        firstName          : first_name
                        middleName         : middle_name
                        state              : state
                        city               : city
                        zipCode            : zip_code
                        country            : country
                        businessPhone      : business_phone
                        bookType           : book_type
                        marketValue        : market_value
                        noOfAccts          : no_of_accts
                        status             : status
                        isPrimary          : is_primary
                        email              : email
                      }
                    ]
                    bookRoles          : rel_acct_adv_book_roles
                    [
                      {
                        advisorId      : advisor_id
                        bookRole       : book_role
                      }
                    ]
                    repCodes           : rel_acct_rep_codes
                    [
                      {
                        advisorRepNumber : advisor_rep_number
                        intType        : int_type
                        repcodeSource  : repcode_source
                      }
                    ]
                    advisorHierarchy   : rel_acct_hierarchy
                    [
                      {
                        partyNodePathNm    : party_node_path_nm
                        partyNodePathValue : party_node_path_value
                      }
                    ]
                    pxInvestorEntitlements : rel_acct_ent_inv_entitlements
                    [
                      {
                        partyRoleId        : party_role_id
                        accountRole        : account_role
                        accountSource      : account_source
                        accountAccessStatus : account_access_status
                        investorId         : investor_id
                        accountRoleCode    : account_role_code
                      }
                    ]
                    advisoryContexts   : rel_acct_advisory_ctx
                    [
                      {
                        advisoryContext : advisory_context
                      }
                    ]
                  }""");

        ddl.add("""
                CREATE OR REPLACE JSON RELATIONAL DUALITY VIEW dv_advisor AS
                  rel_advisor
                  {
                    _id                        : id
                    accountViewableMarketValue : account_viewable_market_value
                    entDataOwnerPartyRoleId    : ent_data_owner_party_role_id
                    advisorName                : advisor_name
                    pxId                       : px_id
                    partyNodeLabelId           : party_node_label_id
                    advisorTaxId               : advisor_tax_id
                    userType                   : user_type
                    finInstId                  : fin_inst_id
                    advState                   : adv_state
                    advisorFullName            : advisor_full_name
                    advSetupTmst               : adv_setup_tmst
                    advUpdateTmst              : adv_update_tmst
                    advAcctMethod              : adv_acct_method
                    advMethodFlag              : adv_method_flag
                    riaIarQuestion             : ria_iar_question
                    dbaQuestion                : dba_question
                    noOfSegments               : no_of_segments
                    finInstName                : fin_inst_name
                    finLastName                : fin_last_name
                    finFirstName               : fin_first_name
                    viewableInvestorCount      : viewable_investor_count
                    accountViewableCount       : account_viewable_count
                    state                      : state
                    city                       : city
                    zip                        : zip
                    country                    : country
                    status                     : status
                    viewableSource             : viewable_source
                    ETLUpdateTS                : etl_update_ts
                    pxClientId                 : px_client_id
                    pxClientName               : px_client_name
                    pxClientRefId              : px_client_ref_id
                    advisorHierarchy           : rel_adv_hierarchy
                    [
                      {
                        partyNodePathNm        : party_node_path_nm
                        partyNodePathValue     : party_node_path_value
                      }
                    ]
                    partyRoleIds               : rel_adv_party_role_ids
                    [
                      {
                        partyRoleId            : party_role_id
                      }
                    ]
                    holdings                   : rel_adv_holdings
                    [
                      {
                        fundId                 : fund_id
                        fundName               : fund_name
                        fundTicker             : fund_ticker
                        mgtName                : mgt_name
                        dividendRate           : dividend_rate
                      }
                    ]
                    repCodes                   : rel_adv_rep_codes
                    [
                      {
                        advisorRepNumber       : advisor_rep_number
                        intType                : int_type
                        repcodeSource          : repcode_source
                      }
                    ]
                    advisoryContexts           : rel_adv_advisory_ctx
                    [
                      {
                        advisoryContext         : advisory_context
                      }
                    ]
                  }""");

        return ddl;
    }

    public List<String> getDropOrder() {
        // Views can be dropped in any order (no FK dependencies between views)
        return getViewNames();
    }
}
