{% macro is_interaccount(col) %}
    {%- set default_patterns = [
        'atmwithdraw',
        'automaticpayment',
        'chasecard',
        'chasecreditcrd',
        'consumeronline',
        'crbkrg',
        'currencycloud',
        'edwardjones',
        'jpmorganchaseexttrnsfr',
        'manualcr',
        'nonchase',
        'non-chase',
        'onlinetransfer',
        'origconamesamuellewisjam',
        'paymentthankyou',
        'paymenttochase',
        'paypaltransfer',
        'realtimetransferrecdfromabacontrbnkfrombnfwiserefmark',
        'returnedpayment',
        'robinhood',
        'stardata',
        'transferfromcd',
        'withdrawal',
        'venmocash',
        'venmopay',
        'wellsfargo',
        'wfcreditcard'
    ] -%}

    {# use the right default name; alphabetize #}
    {%- set pats = (var('interaccount_patterns', default_patterns) | list | sort) -%}

    {# description_clean is already lowercase and alphanumeric #}
    regexp_contains({{ col }}, r'({{ pats | join("|") }})')
{% endmacro %}