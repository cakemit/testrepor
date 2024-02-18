Hello World!
================
Claudia Tanaka
2024-02-18

<br>

# Health expenditure

Data downloaded from [OECD Data
Explorer](https://data-explorer.oecd.org/)

Read .csv and write to .parquet

``` r
# # DATAFLOW  OECD.ELS.HD:DSD_SHA@DF_SHA(1.0)
# # Health expenditure and financing
# df.health_exp <- read_csv("_datasets/OECD.ELS.HD,DSD_SHA@DF_SHA,1.0+all.csv",name_repair="universal")
#
# df.health_exp |>
#   group_by(TIME_PERIOD) |>
#   arrow::write_dataset("_datasets/OECD_HEALTH_EXP/")
```

<br>

Read from .parquet

``` r
oecd.hlth.exp <- open_dataset("_datasets/OECD_HEALTH_EXP/") |>
  select(-c(STRUCTURE, STRUCTURE_ID, STRUCTURE_NAME, ACTION,
            Measure, MEASURE, Currency, Decimals, DECIMALS,
            FINANCING_SCHEME_REV, Revenues.of.financing.schemes,
            FACTOR_PROVISION, Factor.of.provision, Observation.value, Base.period,
            Asset.type, ASSET_TYPE, PRICE_BASE,
            Time.period,
            OBS_STATUS, OBS_STATUS2, OBS_STATUS3, Observation.status.3,
            UNIT_MULT,
            FREQ, Frequency.of.observation)) |>
  rename(iso3c=REF_AREA, country=Reference.area, ano=TIME_PERIOD,
         health_function=7) |>
  mutate(ano = as.integer(ano)) |>
  rename_with(tolower)
```

<br>

Inspect

``` r
glimpse(oecd.hlth.exp)
```

    ## FileSystemDataset with 53 Parquet files (query)
    ## 10,332,221 rows x 20 columns
    ## $ iso3c                <string> "KOR", "KOR", "CAN", "AUT", "KOR", "AUS", "KOR",…
    ## $ country              <string> "Korea", "Korea", "Canada", "Austria", "Korea", …
    ## $ unit_measure         <string> "PT_B1GQ", "PT_EXP_HLTH", "PT_EXP_PRV", "XDC", "…
    ## $ unit.of.measure      <string> "Percentage of GDP", "Percentage of expenditure …
    ## $ financing_scheme     <string> "_T", "HF2", "HF1", "HF1", "_T", "HF2HF3", "HF31…
    ## $ financing.scheme     <string> "Total", "Voluntary health care payment schemes"…
    ## $ health_function      <string> "BASIC", "HC1HC2", "HC6HC7", "_T", "HC1_HC5", "H…
    ## $ health.function      <string> "Basic health care services", "Curative and reha…
    ## $ mode_provision       <string> "_T", "_T", "_T", "_T", "_T", "_T", "_T", "_T", …
    ## $ mode.of.provision    <string> "Total", "Total", "Total", "Total", "Total", "To…
    ## $ provider             <string> "HP6", "_T", "_T", "_T", "HP3", "_T", "_T", "_T"…
    ## $ health.care.provider <string> "Providers of preventive care", "Total", "Total"…
    ## $ price.base           <string> "Not applicable", "Not applicable", "Not applica…
    ## $ obs_value            <double> 0.036, 1.073, 100.000, 3877.534, 5.078, 57.488, …
    ## $ base_per             <double> NA, NA, NA, 2015, NA, NA, NA, NA, NA, 2015, 2015…
    ## $ currency             <string> NA, NA, NA, "EUR", NA, NA, NA, NA, NA, "KRW", NA…
    ## $ observation.status   <string> NA, NA, NA, NA, NA, NA, NA, NA, NA, NA, NA, NA, …
    ## $ observation.status.2 <string> NA, NA, NA, NA, NA, NA, NA, NA, NA, NA, NA, NA, …
    ## $ unit.multiplier      <string> "Units", "Units", "Units", "Millions", "Units", …
    ## $ ano                   <int32> 1971, 1971, 1971, 1971, 1971, 1971, 1971, 1971, …
    ## Call `print()` for query details

<br>

``` r
colSums(is.na(collect(oecd.hlth.exp))) |> as.data.frame() |> rownames_to_column() |> 
  rename(coluna=1,nulos=2) |> 
  filter(nulos>0) |> arrange(desc(nulos))
```

    ##                 coluna    nulos
    ## 1 observation.status.2 10322288
    ## 2   observation.status  9559770
    ## 3             currency  7643392
    ## 4             base_per  6641670

``` r
unique(collect(oecd.hlth.exp)$price.base)
```

    ## [1] "Not applicable"  "Constant prices" "Current prices"

``` r
oecd.hlth.exp |> 
  summarise(n=n(), .by=c(unit_measure, unit.of.measure)) |> 
  arrange(unit_measure) |> 
  collect()
```

    ## # A tibble: 11 × 3
    ##    unit_measure unit.of.measure                                              n
    ##    <chr>        <chr>                                                    <int>
    ##  1 EUR_PPP      Euros, PPP converted                                   1007544
    ##  2 EUR_PPP_PS   Euros per person, PPP converted                        1002475
    ##  3 PT_B1GQ      Percentage of GDP                                       616611
    ##  4 PT_EXP_FNC   Percentage of expenditure in the same function          568606
    ##  5 PT_EXP_FS    Percentage of expenditure in the same financing scheme  561053
    ##  6 PT_EXP_HLTH  Percentage of expenditure on health                     634042
    ##  7 PT_EXP_PRV   Percentage of expenditure in the same provider          570319
    ##  8 USD_PPP      US dollars, PPP converted                              1344483
    ##  9 USD_PPP_PS   US dollars per person, PPP converted                   1338259
    ## 10 XDC          National currency                                      1347114
    ## 11 XDC_PS       National currency per person                           1341715

``` r
oecd.hlth.exp |> 
  summarise(n=n(), .by=c(financing_scheme, financing.scheme)) |> 
  arrange(financing_scheme) |> 
  collect()
```

    ## # A tibble: 16 × 3
    ##    financing_scheme financing.scheme                                         n
    ##    <chr>            <chr>                                                <int>
    ##  1 HF1              Government/compulsory schemes                       904236
    ##  2 HF11             Government schemes                                  707080
    ##  3 HF121            Social health insurance schemes                     433106
    ##  4 HF122            Compulsory private insurance schemes                117949
    ##  5 HF12HF13         Compulsory contributory health insurance schemes    559223
    ##  6 HF2              Voluntary health care payment schemes               571368
    ##  7 HF21             Voluntary health insurance schemes                  439794
    ##  8 HF22             NPISH financing schemes                             248628
    ##  9 HF23             Enterprise financing schemes                        171609
    ## 10 HF2HF3           Voluntary schemes/household out-of-pocket payments  764271
    ## 11 HF3              Household out-of-pocket payments                    606934
    ## 12 HF31             Out-of-pocket excluding cost-sharing                219342
    ## 13 HF32             Cost-sharing with third-party payers                165827
    ## 14 HF4              Rest of the world financing schemes (non-resident)   32154
    ## 15 _T               Total                                              4385749
    ## 16 _U               Unknown                                               4951

``` r
oecd.hlth.exp |> 
  summarise(n=n(), .by=c(health_function, health.function)) |> 
  arrange(health_function) |> 
  collect()
```

    ## # A tibble: 48 × 3
    ##    health_function health.function                                      n
    ##    <chr>           <chr>                                            <int>
    ##  1 BASIC           Basic health care services                      194064
    ##  2 BASIC_PH        Basic health care services and pharmaceuticals  157540
    ##  3 HC0             Other health care services unknown               56978
    ##  4 HC1             Curative care                                   871726
    ##  5 HC131           General curative care                           175062
    ##  6 HC132           Dental curative care                            164315
    ##  7 HC133           Specialised curative care                       146060
    ##  8 HC139           Other curative care                             105292
    ##  9 HC1HC2          Curative and rehabilitative care               1023803
    ## 10 HC1_HC4         Individual health care services                 361939
    ## # ℹ 38 more rows

``` r
oecd.hlth.exp |> 
  summarise(n=n(), .by=c(mode_provision, mode.of.provision)) |> 
  arrange(mode_provision) |> 
  collect()
```

    ## # A tibble: 5 × 3
    ##   mode_provision mode.of.provision       n
    ##   <chr>          <chr>               <int>
    ## 1 DAY            Day                363532
    ## 2 HB             Home-based         408654
    ## 3 HBEDT          Inpatient          667781
    ## 4 HBOUT          Outpatient        1266758
    ## 5 _T             Total             7625496

``` r
oecd.hlth.exp |> 
  summarise(n=n(), .by=c(provider, health.care.provider)) |> 
  arrange(provider) |> 
  collect()
```

    ## # A tibble: 34 × 3
    ##    provider health.care.provider                                            n
    ##    <chr>    <chr>                                                       <int>
    ##  1 HP1      Hospitals                                                  432118
    ##  2 HP11     General hospitals                                          353704
    ##  3 HP12     Mental health hospitals                                    207843
    ##  4 HP13     Specialised hospitals (other than mental health hospitals) 238416
    ##  5 HP2      Residential long-term care facilities                      231269
    ##  6 HP21     Long-term nursing care facilities                          161459
    ##  7 HP22     Mental health and substance abuse faciltites                78058
    ##  8 HP29     Other residential long-term care facilities                 82045
    ##  9 HP3      Providers of ambulatory health care                        454415
    ## 10 HP31     Medical practices                                          323918
    ## # ℹ 24 more rows

<br>

Summarize

``` r
oecd.hlth.exp |> 
  filter(financing_scheme=="_T", mode_provision=="_T", health_function=="_T", provider=="_T",
         unit_measure=="USD_PPP", price.base=="Constant prices") |>
  select(-c(unit_measure, unit.of.measure, price.base)) |> 
  select(ano, country, obs_value) |> collect() |> 
  summarise(value = sum(obs_value), .by=ano) |> 
  ggplot(aes(x=ano, y=value, group=1)) +
  geom_line(color="blue") + geom_point(color="blue") +
  scale_y_continuous(labels=scales::label_number(scale=1/1000), limits=c(0,NA)) +
  labs(
    x=NULL, y=NULL,
    title="Total Health Expenditure, USD billions",
    subtitle="PPP converted at constant prices",
    caption="Source: OECD.Stat, National Accounts - Main aggregates. Extracted June 2023, Paris."
  )
```

![](hello_world_files/figure-gfm/unnamed-chunk-11-1.png)<!-- -->

<br>

``` r
oecd.hlth.exp |> 
  filter(financing_scheme=="_T", mode_provision=="_T", health_function=="_T", provider=="_T",
         unit_measure=="USD_PPP", price.base=="Constant prices") |>
  select(-c(unit_measure, unit.of.measure, price.base)) |> 
  select(ano, country, obs_value) |> collect() |> 
  summarise(value = n_distinct(country), .by=ano) |> 
  ggplot(aes(x=ano, y=value, group=1)) +
  geom_line(color="blue") + geom_point(color="blue") +
  scale_y_continuous(labels=scales::label_number()) +
  labs(
    x=NULL, y=NULL,
    title="Number of countries",
    caption="Source: OECD.Stat, National Accounts - Main aggregates. Extracted June 2023, Paris."
  )
```

![](hello_world_files/figure-gfm/unnamed-chunk-12-1.png)<!-- -->

``` r
oecd.hlth.exp |> 
  filter(ano == 2022, 
         financing_scheme!="_T", mode_provision=="_T", health_function=="_T", provider=="_T",
         unit_measure=="USD_PPP", price.base=="Constant prices") |> 
  summarise(
    countries = n_distinct(iso3c),
    health_usd = sum(obs_value),
    .by=c(financing.scheme)) |> 
  collect() |> 
  mutate(pct = health_usd/sum(health_usd)) |> 
  arrange(desc(health_usd)) |> 
  gt(rowname_col=1, locale="pt") |> sub_missing() |>
  fmt_integer(c(countries,health_usd)) |> fmt_percent(pct, decimals=1) |> 
  gtExtras::gt_plt_bar(health_usd, keep_column=T) |> 
  tab_options(
    heading.align="left", heading.title.font.size=pct(110), heading.subtitle.font.size=pct(90),
    column_labels.font.weight="bold", column_labels.font.size=pct(80),
    column_labels.text_transform="uppercase", column_labels.background.color="gray95",
    data_row.padding=px(2), row_group.padding=px(2), row_group.font.weight="bold",
    grand_summary_row.padding=px(2), grand_summary_row.background.color="gray95",
    table.font.size=pct(90), source_notes.font.size = pct(70)
  ) |> 
  tab_header(title=md("**Health Expenditure by Financing Scheme, 2022**"),
             subtitle="USD Millions PPP converted at constant prices") |> 
  tab_source_note("Source: OECD.Stat, National Accounts - Main aggregates. Extracted June 2023, Paris.")
```

<div id="pjxjoqcfmc" style="padding-left:0px;padding-right:0px;padding-top:10px;padding-bottom:10px;overflow-x:auto;overflow-y:auto;width:auto;height:auto;">
<style>#pjxjoqcfmc table {
  font-family: system-ui, 'Segoe UI', Roboto, Helvetica, Arial, sans-serif, 'Apple Color Emoji', 'Segoe UI Emoji', 'Segoe UI Symbol', 'Noto Color Emoji';
  -webkit-font-smoothing: antialiased;
  -moz-osx-font-smoothing: grayscale;
}
&#10;#pjxjoqcfmc thead, #pjxjoqcfmc tbody, #pjxjoqcfmc tfoot, #pjxjoqcfmc tr, #pjxjoqcfmc td, #pjxjoqcfmc th {
  border-style: none;
}
&#10;#pjxjoqcfmc p {
  margin: 0;
  padding: 0;
}
&#10;#pjxjoqcfmc .gt_table {
  display: table;
  border-collapse: collapse;
  line-height: normal;
  margin-left: auto;
  margin-right: auto;
  color: #333333;
  font-size: 90%;
  font-weight: normal;
  font-style: normal;
  background-color: #FFFFFF;
  width: auto;
  border-top-style: solid;
  border-top-width: 2px;
  border-top-color: #A8A8A8;
  border-right-style: none;
  border-right-width: 2px;
  border-right-color: #D3D3D3;
  border-bottom-style: solid;
  border-bottom-width: 2px;
  border-bottom-color: #A8A8A8;
  border-left-style: none;
  border-left-width: 2px;
  border-left-color: #D3D3D3;
}
&#10;#pjxjoqcfmc .gt_caption {
  padding-top: 4px;
  padding-bottom: 4px;
}
&#10;#pjxjoqcfmc .gt_title {
  color: #333333;
  font-size: 110%;
  font-weight: initial;
  padding-top: 4px;
  padding-bottom: 4px;
  padding-left: 5px;
  padding-right: 5px;
  border-bottom-color: #FFFFFF;
  border-bottom-width: 0;
}
&#10;#pjxjoqcfmc .gt_subtitle {
  color: #333333;
  font-size: 90%;
  font-weight: initial;
  padding-top: 3px;
  padding-bottom: 5px;
  padding-left: 5px;
  padding-right: 5px;
  border-top-color: #FFFFFF;
  border-top-width: 0;
}
&#10;#pjxjoqcfmc .gt_heading {
  background-color: #FFFFFF;
  text-align: left;
  border-bottom-color: #FFFFFF;
  border-left-style: none;
  border-left-width: 1px;
  border-left-color: #D3D3D3;
  border-right-style: none;
  border-right-width: 1px;
  border-right-color: #D3D3D3;
}
&#10;#pjxjoqcfmc .gt_bottom_border {
  border-bottom-style: solid;
  border-bottom-width: 2px;
  border-bottom-color: #D3D3D3;
}
&#10;#pjxjoqcfmc .gt_col_headings {
  border-top-style: solid;
  border-top-width: 2px;
  border-top-color: #D3D3D3;
  border-bottom-style: solid;
  border-bottom-width: 2px;
  border-bottom-color: #D3D3D3;
  border-left-style: none;
  border-left-width: 1px;
  border-left-color: #D3D3D3;
  border-right-style: none;
  border-right-width: 1px;
  border-right-color: #D3D3D3;
}
&#10;#pjxjoqcfmc .gt_col_heading {
  color: #333333;
  background-color: #F2F2F2;
  font-size: 80%;
  font-weight: bold;
  text-transform: uppercase;
  border-left-style: none;
  border-left-width: 1px;
  border-left-color: #D3D3D3;
  border-right-style: none;
  border-right-width: 1px;
  border-right-color: #D3D3D3;
  vertical-align: bottom;
  padding-top: 5px;
  padding-bottom: 6px;
  padding-left: 5px;
  padding-right: 5px;
  overflow-x: hidden;
}
&#10;#pjxjoqcfmc .gt_column_spanner_outer {
  color: #333333;
  background-color: #F2F2F2;
  font-size: 80%;
  font-weight: bold;
  text-transform: uppercase;
  padding-top: 0;
  padding-bottom: 0;
  padding-left: 4px;
  padding-right: 4px;
}
&#10;#pjxjoqcfmc .gt_column_spanner_outer:first-child {
  padding-left: 0;
}
&#10;#pjxjoqcfmc .gt_column_spanner_outer:last-child {
  padding-right: 0;
}
&#10;#pjxjoqcfmc .gt_column_spanner {
  border-bottom-style: solid;
  border-bottom-width: 2px;
  border-bottom-color: #D3D3D3;
  vertical-align: bottom;
  padding-top: 5px;
  padding-bottom: 5px;
  overflow-x: hidden;
  display: inline-block;
  width: 100%;
}
&#10;#pjxjoqcfmc .gt_spanner_row {
  border-bottom-style: hidden;
}
&#10;#pjxjoqcfmc .gt_group_heading {
  padding-top: 2px;
  padding-bottom: 2px;
  padding-left: 5px;
  padding-right: 5px;
  color: #333333;
  background-color: #FFFFFF;
  font-size: 100%;
  font-weight: bold;
  text-transform: inherit;
  border-top-style: solid;
  border-top-width: 2px;
  border-top-color: #D3D3D3;
  border-bottom-style: solid;
  border-bottom-width: 2px;
  border-bottom-color: #D3D3D3;
  border-left-style: none;
  border-left-width: 1px;
  border-left-color: #D3D3D3;
  border-right-style: none;
  border-right-width: 1px;
  border-right-color: #D3D3D3;
  vertical-align: middle;
  text-align: left;
}
&#10;#pjxjoqcfmc .gt_empty_group_heading {
  padding: 0.5px;
  color: #333333;
  background-color: #FFFFFF;
  font-size: 100%;
  font-weight: bold;
  border-top-style: solid;
  border-top-width: 2px;
  border-top-color: #D3D3D3;
  border-bottom-style: solid;
  border-bottom-width: 2px;
  border-bottom-color: #D3D3D3;
  vertical-align: middle;
}
&#10;#pjxjoqcfmc .gt_from_md > :first-child {
  margin-top: 0;
}
&#10;#pjxjoqcfmc .gt_from_md > :last-child {
  margin-bottom: 0;
}
&#10;#pjxjoqcfmc .gt_row {
  padding-top: 2px;
  padding-bottom: 2px;
  padding-left: 5px;
  padding-right: 5px;
  margin: 10px;
  border-top-style: solid;
  border-top-width: 1px;
  border-top-color: #D3D3D3;
  border-left-style: none;
  border-left-width: 1px;
  border-left-color: #D3D3D3;
  border-right-style: none;
  border-right-width: 1px;
  border-right-color: #D3D3D3;
  vertical-align: middle;
  overflow-x: hidden;
}
&#10;#pjxjoqcfmc .gt_stub {
  color: #333333;
  background-color: #FFFFFF;
  font-size: 100%;
  font-weight: initial;
  text-transform: inherit;
  border-right-style: solid;
  border-right-width: 2px;
  border-right-color: #D3D3D3;
  padding-left: 5px;
  padding-right: 5px;
}
&#10;#pjxjoqcfmc .gt_stub_row_group {
  color: #333333;
  background-color: #FFFFFF;
  font-size: 100%;
  font-weight: initial;
  text-transform: inherit;
  border-right-style: solid;
  border-right-width: 2px;
  border-right-color: #D3D3D3;
  padding-left: 5px;
  padding-right: 5px;
  vertical-align: top;
}
&#10;#pjxjoqcfmc .gt_row_group_first td {
  border-top-width: 2px;
}
&#10;#pjxjoqcfmc .gt_row_group_first th {
  border-top-width: 2px;
}
&#10;#pjxjoqcfmc .gt_summary_row {
  color: #333333;
  background-color: #FFFFFF;
  text-transform: inherit;
  padding-top: 8px;
  padding-bottom: 8px;
  padding-left: 5px;
  padding-right: 5px;
}
&#10;#pjxjoqcfmc .gt_first_summary_row {
  border-top-style: solid;
  border-top-color: #D3D3D3;
}
&#10;#pjxjoqcfmc .gt_first_summary_row.thick {
  border-top-width: 2px;
}
&#10;#pjxjoqcfmc .gt_last_summary_row {
  padding-top: 8px;
  padding-bottom: 8px;
  padding-left: 5px;
  padding-right: 5px;
  border-bottom-style: solid;
  border-bottom-width: 2px;
  border-bottom-color: #D3D3D3;
}
&#10;#pjxjoqcfmc .gt_grand_summary_row {
  color: #333333;
  background-color: #F2F2F2;
  text-transform: inherit;
  padding-top: 2px;
  padding-bottom: 2px;
  padding-left: 5px;
  padding-right: 5px;
}
&#10;#pjxjoqcfmc .gt_first_grand_summary_row {
  padding-top: 2px;
  padding-bottom: 2px;
  padding-left: 5px;
  padding-right: 5px;
  border-top-style: double;
  border-top-width: 6px;
  border-top-color: #D3D3D3;
}
&#10;#pjxjoqcfmc .gt_last_grand_summary_row_top {
  padding-top: 2px;
  padding-bottom: 2px;
  padding-left: 5px;
  padding-right: 5px;
  border-bottom-style: double;
  border-bottom-width: 6px;
  border-bottom-color: #D3D3D3;
}
&#10;#pjxjoqcfmc .gt_striped {
  background-color: rgba(128, 128, 128, 0.05);
}
&#10;#pjxjoqcfmc .gt_table_body {
  border-top-style: solid;
  border-top-width: 2px;
  border-top-color: #D3D3D3;
  border-bottom-style: solid;
  border-bottom-width: 2px;
  border-bottom-color: #D3D3D3;
}
&#10;#pjxjoqcfmc .gt_footnotes {
  color: #333333;
  background-color: #FFFFFF;
  border-bottom-style: none;
  border-bottom-width: 2px;
  border-bottom-color: #D3D3D3;
  border-left-style: none;
  border-left-width: 2px;
  border-left-color: #D3D3D3;
  border-right-style: none;
  border-right-width: 2px;
  border-right-color: #D3D3D3;
}
&#10;#pjxjoqcfmc .gt_footnote {
  margin: 0px;
  font-size: 90%;
  padding-top: 4px;
  padding-bottom: 4px;
  padding-left: 5px;
  padding-right: 5px;
}
&#10;#pjxjoqcfmc .gt_sourcenotes {
  color: #333333;
  background-color: #FFFFFF;
  border-bottom-style: none;
  border-bottom-width: 2px;
  border-bottom-color: #D3D3D3;
  border-left-style: none;
  border-left-width: 2px;
  border-left-color: #D3D3D3;
  border-right-style: none;
  border-right-width: 2px;
  border-right-color: #D3D3D3;
}
&#10;#pjxjoqcfmc .gt_sourcenote {
  font-size: 70%;
  padding-top: 4px;
  padding-bottom: 4px;
  padding-left: 5px;
  padding-right: 5px;
}
&#10;#pjxjoqcfmc .gt_left {
  text-align: left;
}
&#10;#pjxjoqcfmc .gt_center {
  text-align: center;
}
&#10;#pjxjoqcfmc .gt_right {
  text-align: right;
  font-variant-numeric: tabular-nums;
}
&#10;#pjxjoqcfmc .gt_font_normal {
  font-weight: normal;
}
&#10;#pjxjoqcfmc .gt_font_bold {
  font-weight: bold;
}
&#10;#pjxjoqcfmc .gt_font_italic {
  font-style: italic;
}
&#10;#pjxjoqcfmc .gt_super {
  font-size: 65%;
}
&#10;#pjxjoqcfmc .gt_footnote_marks {
  font-size: 75%;
  vertical-align: 0.4em;
  position: initial;
}
&#10;#pjxjoqcfmc .gt_asterisk {
  font-size: 100%;
  vertical-align: 0;
}
&#10;#pjxjoqcfmc .gt_indent_1 {
  text-indent: 5px;
}
&#10;#pjxjoqcfmc .gt_indent_2 {
  text-indent: 10px;
}
&#10;#pjxjoqcfmc .gt_indent_3 {
  text-indent: 15px;
}
&#10;#pjxjoqcfmc .gt_indent_4 {
  text-indent: 20px;
}
&#10;#pjxjoqcfmc .gt_indent_5 {
  text-indent: 25px;
}
</style>
<table class="gt_table" data-quarto-disable-processing="false" data-quarto-bootstrap="false">
  <thead>
    <tr class="gt_heading">
      <td colspan="5" class="gt_heading gt_title gt_font_normal" style><strong>Health Expenditure by Financing Scheme, 2022</strong></td>
    </tr>
    <tr class="gt_heading">
      <td colspan="5" class="gt_heading gt_subtitle gt_font_normal gt_bottom_border" style>USD Millions PPP converted at constant prices</td>
    </tr>
    <tr class="gt_col_headings">
      <th class="gt_col_heading gt_columns_bottom_border gt_left" rowspan="1" colspan="1" scope="col" id="financing.scheme">financing.scheme</th>
      <th class="gt_col_heading gt_columns_bottom_border gt_right" rowspan="1" colspan="1" scope="col" id="countries">countries</th>
      <th class="gt_col_heading gt_columns_bottom_border gt_right" rowspan="1" colspan="1" scope="col" id="health_usd">health_usd</th>
      <th class="gt_col_heading gt_columns_bottom_border gt_right" rowspan="1" colspan="1" scope="col" id="pct">pct</th>
      <th class="gt_col_heading gt_columns_bottom_border gt_left" rowspan="1" colspan="1" scope="col" id="health_usd">health_usd</th>
    </tr>
  </thead>
  <tbody class="gt_table_body">
    <tr><td headers="financing.scheme" class="gt_row gt_left">Government/compulsory schemes</td>
<td headers="countries" class="gt_row gt_right">38</td>
<td headers="health_usd" class="gt_row gt_right">6.003.043</td>
<td headers="pct" class="gt_row gt_right">64,7%</td>
<td headers="DUPE_COLUMN_PLT" class="gt_row gt_left"><?xml version='1.0' encoding='UTF-8' ?><svg xmlns='http://www.w3.org/2000/svg' xmlns:xlink='http://www.w3.org/1999/xlink' class='svglite' width='113.39pt' height='14.17pt' viewBox='0 0 113.39 14.17'><defs>  <style type='text/css'><![CDATA[    .svglite line, .svglite polyline, .svglite polygon, .svglite path, .svglite rect, .svglite circle {      fill: none;      stroke: #000000;      stroke-linecap: round;      stroke-linejoin: round;      stroke-miterlimit: 10.00;    }    .svglite text {      white-space: pre;    }  ]]></style></defs><rect width='100%' height='100%' style='stroke: none; fill: none;'/><defs>  <clipPath id='cpMC4wMHwxMTMuMzl8MC4wMHwxNC4xNw=='>    <rect x='0.00' y='0.00' width='113.39' height='14.17' />  </clipPath></defs><g clip-path='url(#cpMC4wMHwxMTMuMzl8MC4wMHwxNC4xNw==)'><rect x='5.02' y='0.89' width='98.37' height='12.40' style='stroke-width: 1.07; stroke: none; stroke-linecap: butt; stroke-linejoin: miter; fill: #A020F0;' /><line x1='5.02' y1='14.17' x2='5.02' y2='0.0000000000000018' style='stroke-width: 1.07; stroke-linecap: butt;' /></g></svg></td></tr>
    <tr><td headers="financing.scheme" class="gt_row gt_left">Voluntary schemes/household out-of-pocket payments</td>
<td headers="countries" class="gt_row gt_right">38</td>
<td headers="health_usd" class="gt_row gt_right">1.384.586</td>
<td headers="pct" class="gt_row gt_right">14,9%</td>
<td headers="DUPE_COLUMN_PLT" class="gt_row gt_left"><?xml version='1.0' encoding='UTF-8' ?><svg xmlns='http://www.w3.org/2000/svg' xmlns:xlink='http://www.w3.org/1999/xlink' class='svglite' width='113.39pt' height='14.17pt' viewBox='0 0 113.39 14.17'><defs>  <style type='text/css'><![CDATA[    .svglite line, .svglite polyline, .svglite polygon, .svglite path, .svglite rect, .svglite circle {      fill: none;      stroke: #000000;      stroke-linecap: round;      stroke-linejoin: round;      stroke-miterlimit: 10.00;    }    .svglite text {      white-space: pre;    }  ]]></style></defs><rect width='100%' height='100%' style='stroke: none; fill: none;'/><defs>  <clipPath id='cpMC4wMHwxMTMuMzl8MC4wMHwxNC4xNw=='>    <rect x='0.00' y='0.00' width='113.39' height='14.17' />  </clipPath></defs><g clip-path='url(#cpMC4wMHwxMTMuMzl8MC4wMHwxNC4xNw==)'><rect x='5.02' y='0.89' width='22.69' height='12.40' style='stroke-width: 1.07; stroke: none; stroke-linecap: butt; stroke-linejoin: miter; fill: #A020F0;' /><line x1='5.02' y1='14.17' x2='5.02' y2='0.0000000000000018' style='stroke-width: 1.07; stroke-linecap: butt;' /></g></svg></td></tr>
    <tr><td headers="financing.scheme" class="gt_row gt_left">Government schemes</td>
<td headers="countries" class="gt_row gt_right">15</td>
<td headers="health_usd" class="gt_row gt_right">703.797</td>
<td headers="pct" class="gt_row gt_right">7,6%</td>
<td headers="DUPE_COLUMN_PLT" class="gt_row gt_left"><?xml version='1.0' encoding='UTF-8' ?><svg xmlns='http://www.w3.org/2000/svg' xmlns:xlink='http://www.w3.org/1999/xlink' class='svglite' width='113.39pt' height='14.17pt' viewBox='0 0 113.39 14.17'><defs>  <style type='text/css'><![CDATA[    .svglite line, .svglite polyline, .svglite polygon, .svglite path, .svglite rect, .svglite circle {      fill: none;      stroke: #000000;      stroke-linecap: round;      stroke-linejoin: round;      stroke-miterlimit: 10.00;    }    .svglite text {      white-space: pre;    }  ]]></style></defs><rect width='100%' height='100%' style='stroke: none; fill: none;'/><defs>  <clipPath id='cpMC4wMHwxMTMuMzl8MC4wMHwxNC4xNw=='>    <rect x='0.00' y='0.00' width='113.39' height='14.17' />  </clipPath></defs><g clip-path='url(#cpMC4wMHwxMTMuMzl8MC4wMHwxNC4xNw==)'><rect x='5.02' y='0.89' width='11.53' height='12.40' style='stroke-width: 1.07; stroke: none; stroke-linecap: butt; stroke-linejoin: miter; fill: #A020F0;' /><line x1='5.02' y1='14.17' x2='5.02' y2='0.0000000000000018' style='stroke-width: 1.07; stroke-linecap: butt;' /></g></svg></td></tr>
    <tr><td headers="financing.scheme" class="gt_row gt_left">Household out-of-pocket payments</td>
<td headers="countries" class="gt_row gt_right">19</td>
<td headers="health_usd" class="gt_row gt_right">318.546</td>
<td headers="pct" class="gt_row gt_right">3,4%</td>
<td headers="DUPE_COLUMN_PLT" class="gt_row gt_left"><?xml version='1.0' encoding='UTF-8' ?><svg xmlns='http://www.w3.org/2000/svg' xmlns:xlink='http://www.w3.org/1999/xlink' class='svglite' width='113.39pt' height='14.17pt' viewBox='0 0 113.39 14.17'><defs>  <style type='text/css'><![CDATA[    .svglite line, .svglite polyline, .svglite polygon, .svglite path, .svglite rect, .svglite circle {      fill: none;      stroke: #000000;      stroke-linecap: round;      stroke-linejoin: round;      stroke-miterlimit: 10.00;    }    .svglite text {      white-space: pre;    }  ]]></style></defs><rect width='100%' height='100%' style='stroke: none; fill: none;'/><defs>  <clipPath id='cpMC4wMHwxMTMuMzl8MC4wMHwxNC4xNw=='>    <rect x='0.00' y='0.00' width='113.39' height='14.17' />  </clipPath></defs><g clip-path='url(#cpMC4wMHwxMTMuMzl8MC4wMHwxNC4xNw==)'><rect x='5.02' y='0.89' width='5.22' height='12.40' style='stroke-width: 1.07; stroke: none; stroke-linecap: butt; stroke-linejoin: miter; fill: #A020F0;' /><line x1='5.02' y1='14.17' x2='5.02' y2='0.0000000000000018' style='stroke-width: 1.07; stroke-linecap: butt;' /></g></svg></td></tr>
    <tr><td headers="financing.scheme" class="gt_row gt_left">Compulsory contributory health insurance schemes</td>
<td headers="countries" class="gt_row gt_right">11</td>
<td headers="health_usd" class="gt_row gt_right">281.560</td>
<td headers="pct" class="gt_row gt_right">3,0%</td>
<td headers="DUPE_COLUMN_PLT" class="gt_row gt_left"><?xml version='1.0' encoding='UTF-8' ?><svg xmlns='http://www.w3.org/2000/svg' xmlns:xlink='http://www.w3.org/1999/xlink' class='svglite' width='113.39pt' height='14.17pt' viewBox='0 0 113.39 14.17'><defs>  <style type='text/css'><![CDATA[    .svglite line, .svglite polyline, .svglite polygon, .svglite path, .svglite rect, .svglite circle {      fill: none;      stroke: #000000;      stroke-linecap: round;      stroke-linejoin: round;      stroke-miterlimit: 10.00;    }    .svglite text {      white-space: pre;    }  ]]></style></defs><rect width='100%' height='100%' style='stroke: none; fill: none;'/><defs>  <clipPath id='cpMC4wMHwxMTMuMzl8MC4wMHwxNC4xNw=='>    <rect x='0.00' y='0.00' width='113.39' height='14.17' />  </clipPath></defs><g clip-path='url(#cpMC4wMHwxMTMuMzl8MC4wMHwxNC4xNw==)'><rect x='5.02' y='0.89' width='4.61' height='12.40' style='stroke-width: 1.07; stroke: none; stroke-linecap: butt; stroke-linejoin: miter; fill: #A020F0;' /><line x1='5.02' y1='14.17' x2='5.02' y2='0.0000000000000018' style='stroke-width: 1.07; stroke-linecap: butt;' /></g></svg></td></tr>
    <tr><td headers="financing.scheme" class="gt_row gt_left">Social health insurance schemes</td>
<td headers="countries" class="gt_row gt_right">11</td>
<td headers="health_usd" class="gt_row gt_right">226.801</td>
<td headers="pct" class="gt_row gt_right">2,4%</td>
<td headers="DUPE_COLUMN_PLT" class="gt_row gt_left"><?xml version='1.0' encoding='UTF-8' ?><svg xmlns='http://www.w3.org/2000/svg' xmlns:xlink='http://www.w3.org/1999/xlink' class='svglite' width='113.39pt' height='14.17pt' viewBox='0 0 113.39 14.17'><defs>  <style type='text/css'><![CDATA[    .svglite line, .svglite polyline, .svglite polygon, .svglite path, .svglite rect, .svglite circle {      fill: none;      stroke: #000000;      stroke-linecap: round;      stroke-linejoin: round;      stroke-miterlimit: 10.00;    }    .svglite text {      white-space: pre;    }  ]]></style></defs><rect width='100%' height='100%' style='stroke: none; fill: none;'/><defs>  <clipPath id='cpMC4wMHwxMTMuMzl8MC4wMHwxNC4xNw=='>    <rect x='0.00' y='0.00' width='113.39' height='14.17' />  </clipPath></defs><g clip-path='url(#cpMC4wMHwxMTMuMzl8MC4wMHwxNC4xNw==)'><rect x='5.02' y='0.89' width='3.72' height='12.40' style='stroke-width: 1.07; stroke: none; stroke-linecap: butt; stroke-linejoin: miter; fill: #A020F0;' /><line x1='5.02' y1='14.17' x2='5.02' y2='0.0000000000000018' style='stroke-width: 1.07; stroke-linecap: butt;' /></g></svg></td></tr>
    <tr><td headers="financing.scheme" class="gt_row gt_left">Voluntary health care payment schemes</td>
<td headers="countries" class="gt_row gt_right">18</td>
<td headers="health_usd" class="gt_row gt_right">109.065</td>
<td headers="pct" class="gt_row gt_right">1,2%</td>
<td headers="DUPE_COLUMN_PLT" class="gt_row gt_left"><?xml version='1.0' encoding='UTF-8' ?><svg xmlns='http://www.w3.org/2000/svg' xmlns:xlink='http://www.w3.org/1999/xlink' class='svglite' width='113.39pt' height='14.17pt' viewBox='0 0 113.39 14.17'><defs>  <style type='text/css'><![CDATA[    .svglite line, .svglite polyline, .svglite polygon, .svglite path, .svglite rect, .svglite circle {      fill: none;      stroke: #000000;      stroke-linecap: round;      stroke-linejoin: round;      stroke-miterlimit: 10.00;    }    .svglite text {      white-space: pre;    }  ]]></style></defs><rect width='100%' height='100%' style='stroke: none; fill: none;'/><defs>  <clipPath id='cpMC4wMHwxMTMuMzl8MC4wMHwxNC4xNw=='>    <rect x='0.00' y='0.00' width='113.39' height='14.17' />  </clipPath></defs><g clip-path='url(#cpMC4wMHwxMTMuMzl8MC4wMHwxNC4xNw==)'><rect x='5.02' y='0.89' width='1.79' height='12.40' style='stroke-width: 1.07; stroke: none; stroke-linecap: butt; stroke-linejoin: miter; fill: #A020F0;' /><line x1='5.02' y1='14.17' x2='5.02' y2='0.0000000000000018' style='stroke-width: 1.07; stroke-linecap: butt;' /></g></svg></td></tr>
    <tr><td headers="financing.scheme" class="gt_row gt_left">Voluntary health insurance schemes</td>
<td headers="countries" class="gt_row gt_right">13</td>
<td headers="health_usd" class="gt_row gt_right">71.956</td>
<td headers="pct" class="gt_row gt_right">0,8%</td>
<td headers="DUPE_COLUMN_PLT" class="gt_row gt_left"><?xml version='1.0' encoding='UTF-8' ?><svg xmlns='http://www.w3.org/2000/svg' xmlns:xlink='http://www.w3.org/1999/xlink' class='svglite' width='113.39pt' height='14.17pt' viewBox='0 0 113.39 14.17'><defs>  <style type='text/css'><![CDATA[    .svglite line, .svglite polyline, .svglite polygon, .svglite path, .svglite rect, .svglite circle {      fill: none;      stroke: #000000;      stroke-linecap: round;      stroke-linejoin: round;      stroke-miterlimit: 10.00;    }    .svglite text {      white-space: pre;    }  ]]></style></defs><rect width='100%' height='100%' style='stroke: none; fill: none;'/><defs>  <clipPath id='cpMC4wMHwxMTMuMzl8MC4wMHwxNC4xNw=='>    <rect x='0.00' y='0.00' width='113.39' height='14.17' />  </clipPath></defs><g clip-path='url(#cpMC4wMHwxMTMuMzl8MC4wMHwxNC4xNw==)'><rect x='5.02' y='0.89' width='1.18' height='12.40' style='stroke-width: 1.07; stroke: none; stroke-linecap: butt; stroke-linejoin: miter; fill: #A020F0;' /><line x1='5.02' y1='14.17' x2='5.02' y2='0.0000000000000018' style='stroke-width: 1.07; stroke-linecap: butt;' /></g></svg></td></tr>
    <tr><td headers="financing.scheme" class="gt_row gt_left">Out-of-pocket excluding cost-sharing</td>
<td headers="countries" class="gt_row gt_right">6</td>
<td headers="health_usd" class="gt_row gt_right">66.049</td>
<td headers="pct" class="gt_row gt_right">0,7%</td>
<td headers="DUPE_COLUMN_PLT" class="gt_row gt_left"><?xml version='1.0' encoding='UTF-8' ?><svg xmlns='http://www.w3.org/2000/svg' xmlns:xlink='http://www.w3.org/1999/xlink' class='svglite' width='113.39pt' height='14.17pt' viewBox='0 0 113.39 14.17'><defs>  <style type='text/css'><![CDATA[    .svglite line, .svglite polyline, .svglite polygon, .svglite path, .svglite rect, .svglite circle {      fill: none;      stroke: #000000;      stroke-linecap: round;      stroke-linejoin: round;      stroke-miterlimit: 10.00;    }    .svglite text {      white-space: pre;    }  ]]></style></defs><rect width='100%' height='100%' style='stroke: none; fill: none;'/><defs>  <clipPath id='cpMC4wMHwxMTMuMzl8MC4wMHwxNC4xNw=='>    <rect x='0.00' y='0.00' width='113.39' height='14.17' />  </clipPath></defs><g clip-path='url(#cpMC4wMHwxMTMuMzl8MC4wMHwxNC4xNw==)'><rect x='5.02' y='0.89' width='1.08' height='12.40' style='stroke-width: 1.07; stroke: none; stroke-linecap: butt; stroke-linejoin: miter; fill: #A020F0;' /><line x1='5.02' y1='14.17' x2='5.02' y2='0.0000000000000018' style='stroke-width: 1.07; stroke-linecap: butt;' /></g></svg></td></tr>
    <tr><td headers="financing.scheme" class="gt_row gt_left">Compulsory private insurance schemes</td>
<td headers="countries" class="gt_row gt_right">4</td>
<td headers="health_usd" class="gt_row gt_right">54.759</td>
<td headers="pct" class="gt_row gt_right">0,6%</td>
<td headers="DUPE_COLUMN_PLT" class="gt_row gt_left"><?xml version='1.0' encoding='UTF-8' ?><svg xmlns='http://www.w3.org/2000/svg' xmlns:xlink='http://www.w3.org/1999/xlink' class='svglite' width='113.39pt' height='14.17pt' viewBox='0 0 113.39 14.17'><defs>  <style type='text/css'><![CDATA[    .svglite line, .svglite polyline, .svglite polygon, .svglite path, .svglite rect, .svglite circle {      fill: none;      stroke: #000000;      stroke-linecap: round;      stroke-linejoin: round;      stroke-miterlimit: 10.00;    }    .svglite text {      white-space: pre;    }  ]]></style></defs><rect width='100%' height='100%' style='stroke: none; fill: none;'/><defs>  <clipPath id='cpMC4wMHwxMTMuMzl8MC4wMHwxNC4xNw=='>    <rect x='0.00' y='0.00' width='113.39' height='14.17' />  </clipPath></defs><g clip-path='url(#cpMC4wMHwxMTMuMzl8MC4wMHwxNC4xNw==)'><rect x='5.02' y='0.89' width='0.90' height='12.40' style='stroke-width: 1.07; stroke: none; stroke-linecap: butt; stroke-linejoin: miter; fill: #A020F0;' /><line x1='5.02' y1='14.17' x2='5.02' y2='0.0000000000000018' style='stroke-width: 1.07; stroke-linecap: butt;' /></g></svg></td></tr>
    <tr><td headers="financing.scheme" class="gt_row gt_left">Cost-sharing with third-party payers</td>
<td headers="countries" class="gt_row gt_right">4</td>
<td headers="health_usd" class="gt_row gt_right">35.491</td>
<td headers="pct" class="gt_row gt_right">0,4%</td>
<td headers="DUPE_COLUMN_PLT" class="gt_row gt_left"><?xml version='1.0' encoding='UTF-8' ?><svg xmlns='http://www.w3.org/2000/svg' xmlns:xlink='http://www.w3.org/1999/xlink' class='svglite' width='113.39pt' height='14.17pt' viewBox='0 0 113.39 14.17'><defs>  <style type='text/css'><![CDATA[    .svglite line, .svglite polyline, .svglite polygon, .svglite path, .svglite rect, .svglite circle {      fill: none;      stroke: #000000;      stroke-linecap: round;      stroke-linejoin: round;      stroke-miterlimit: 10.00;    }    .svglite text {      white-space: pre;    }  ]]></style></defs><rect width='100%' height='100%' style='stroke: none; fill: none;'/><defs>  <clipPath id='cpMC4wMHwxMTMuMzl8MC4wMHwxNC4xNw=='>    <rect x='0.00' y='0.00' width='113.39' height='14.17' />  </clipPath></defs><g clip-path='url(#cpMC4wMHwxMTMuMzl8MC4wMHwxNC4xNw==)'><rect x='5.02' y='0.89' width='0.58' height='12.40' style='stroke-width: 1.07; stroke: none; stroke-linecap: butt; stroke-linejoin: miter; fill: #A020F0;' /><line x1='5.02' y1='14.17' x2='5.02' y2='0.0000000000000018' style='stroke-width: 1.07; stroke-linecap: butt;' /></g></svg></td></tr>
    <tr><td headers="financing.scheme" class="gt_row gt_left">NPISH financing schemes</td>
<td headers="countries" class="gt_row gt_right">13</td>
<td headers="health_usd" class="gt_row gt_right">11.668</td>
<td headers="pct" class="gt_row gt_right">0,1%</td>
<td headers="DUPE_COLUMN_PLT" class="gt_row gt_left"><?xml version='1.0' encoding='UTF-8' ?><svg xmlns='http://www.w3.org/2000/svg' xmlns:xlink='http://www.w3.org/1999/xlink' class='svglite' width='113.39pt' height='14.17pt' viewBox='0 0 113.39 14.17'><defs>  <style type='text/css'><![CDATA[    .svglite line, .svglite polyline, .svglite polygon, .svglite path, .svglite rect, .svglite circle {      fill: none;      stroke: #000000;      stroke-linecap: round;      stroke-linejoin: round;      stroke-miterlimit: 10.00;    }    .svglite text {      white-space: pre;    }  ]]></style></defs><rect width='100%' height='100%' style='stroke: none; fill: none;'/><defs>  <clipPath id='cpMC4wMHwxMTMuMzl8MC4wMHwxNC4xNw=='>    <rect x='0.00' y='0.00' width='113.39' height='14.17' />  </clipPath></defs><g clip-path='url(#cpMC4wMHwxMTMuMzl8MC4wMHwxNC4xNw==)'><rect x='5.02' y='0.89' width='0.19' height='12.40' style='stroke-width: 1.07; stroke: none; stroke-linecap: butt; stroke-linejoin: miter; fill: #A020F0;' /><line x1='5.02' y1='14.17' x2='5.02' y2='0.0000000000000018' style='stroke-width: 1.07; stroke-linecap: butt;' /></g></svg></td></tr>
    <tr><td headers="financing.scheme" class="gt_row gt_left">Enterprise financing schemes</td>
<td headers="countries" class="gt_row gt_right">10</td>
<td headers="health_usd" class="gt_row gt_right">5.535</td>
<td headers="pct" class="gt_row gt_right">0,1%</td>
<td headers="DUPE_COLUMN_PLT" class="gt_row gt_left"><?xml version='1.0' encoding='UTF-8' ?><svg xmlns='http://www.w3.org/2000/svg' xmlns:xlink='http://www.w3.org/1999/xlink' class='svglite' width='113.39pt' height='14.17pt' viewBox='0 0 113.39 14.17'><defs>  <style type='text/css'><![CDATA[    .svglite line, .svglite polyline, .svglite polygon, .svglite path, .svglite rect, .svglite circle {      fill: none;      stroke: #000000;      stroke-linecap: round;      stroke-linejoin: round;      stroke-miterlimit: 10.00;    }    .svglite text {      white-space: pre;    }  ]]></style></defs><rect width='100%' height='100%' style='stroke: none; fill: none;'/><defs>  <clipPath id='cpMC4wMHwxMTMuMzl8MC4wMHwxNC4xNw=='>    <rect x='0.00' y='0.00' width='113.39' height='14.17' />  </clipPath></defs><g clip-path='url(#cpMC4wMHwxMTMuMzl8MC4wMHwxNC4xNw==)'><rect x='5.02' y='0.89' width='0.091' height='12.40' style='stroke-width: 1.07; stroke: none; stroke-linecap: butt; stroke-linejoin: miter; fill: #A020F0;' /><line x1='5.02' y1='14.17' x2='5.02' y2='0.0000000000000018' style='stroke-width: 1.07; stroke-linecap: butt;' /></g></svg></td></tr>
    <tr><td headers="financing.scheme" class="gt_row gt_left">Rest of the world financing schemes (non-resident)</td>
<td headers="countries" class="gt_row gt_right">2</td>
<td headers="health_usd" class="gt_row gt_right">44</td>
<td headers="pct" class="gt_row gt_right">0,0%</td>
<td headers="DUPE_COLUMN_PLT" class="gt_row gt_left"><?xml version='1.0' encoding='UTF-8' ?><svg xmlns='http://www.w3.org/2000/svg' xmlns:xlink='http://www.w3.org/1999/xlink' class='svglite' width='113.39pt' height='14.17pt' viewBox='0 0 113.39 14.17'><defs>  <style type='text/css'><![CDATA[    .svglite line, .svglite polyline, .svglite polygon, .svglite path, .svglite rect, .svglite circle {      fill: none;      stroke: #000000;      stroke-linecap: round;      stroke-linejoin: round;      stroke-miterlimit: 10.00;    }    .svglite text {      white-space: pre;    }  ]]></style></defs><rect width='100%' height='100%' style='stroke: none; fill: none;'/><defs>  <clipPath id='cpMC4wMHwxMTMuMzl8MC4wMHwxNC4xNw=='>    <rect x='0.00' y='0.00' width='113.39' height='14.17' />  </clipPath></defs><g clip-path='url(#cpMC4wMHwxMTMuMzl8MC4wMHwxNC4xNw==)'><rect x='5.02' y='0.89' width='0.00071' height='12.40' style='stroke-width: 1.07; stroke: none; stroke-linecap: butt; stroke-linejoin: miter; fill: #A020F0;' /><line x1='5.02' y1='14.17' x2='5.02' y2='0.0000000000000018' style='stroke-width: 1.07; stroke-linecap: butt;' /></g></svg></td></tr>
  </tbody>
  <tfoot class="gt_sourcenotes">
    <tr>
      <td class="gt_sourcenote" colspan="5">Source: OECD.Stat, National Accounts - Main aggregates. Extracted June 2023, Paris.</td>
    </tr>
  </tfoot>
  &#10;</table>
</div>

# Coverage

``` r
# DATAFLOW  OECD.ELS.HD:DSD_HEALTH_PROT@DF_HEALTH_PROT(1.0)
# Social protection
# HIC - Health insurance coverage
oecd.cvrg <- read_csv("_datasets/OECD.HI_coverage.csv", name_repair="universal") |>
  select(-c(STRUCTURE, STRUCTURE_ID, STRUCTURE_NAME, ACTION,
            FREQ, Frequency.of.observation,
            Measure, MEASURE, UNIT_MEASURE, Unit.of.measure,
            INSURANCE_TYPE,
            Time.period, Observation.value,
            OBS_STATUS,
            UNIT_MULT, Unit.multiplier,
            Decimals, DECIMALS)) |>
  rename(iso3c=REF_AREA, country=Reference.area, ano=TIME_PERIOD) |>
  mutate(ano = as.integer(ano)) |>
  rename_with(tolower)
```

<br>

Inspect

``` r
glimpse(oecd.cvrg)
```

    ## Rows: 6,410
    ## Columns: 6
    ## $ iso3c              <chr> "NLD", "NLD", "NLD", "NLD", "NLD", "NLD", "NLD", "N…
    ## $ country            <chr> "Netherlands", "Netherlands", "Netherlands", "Nethe…
    ## $ insurance.type     <chr> "Public and primary voluntary health insurance", "P…
    ## $ ano                <int> 1960, 1961, 1962, 1963, 1964, 1965, 1966, 1967, 196…
    ## $ obs_value          <dbl> 71.0, 71.0, 71.0, 71.0, 71.0, 71.0, 69.9, 69.6, 69.…
    ## $ observation.status <chr> "Normal value", "Normal value", "Normal value", "No…

``` r
range(oecd.cvrg$ano)
```

    ## [1] 1960 2022

``` r
oecd.cvrg |> summarise(n=n(), .by=c(insurance.type))
```

    ## # A tibble: 7 × 2
    ##   insurance.type                                    n
    ##   <chr>                                         <int>
    ## 1 Public and primary voluntary health insurance  1724
    ## 2 Total voluntary health insurance                693
    ## 3 Primary voluntary health insurance              637
    ## 4 Complementary voluntary health insurance        578
    ## 5 Supplementary voluntary health insurance        516
    ## 6 Government/compulsory health insurance         1724
    ## 7 Duplicate voluntary health insurance            538

<br>

Summarize

``` r
oecd.cvrg |> 
  filter(ano == 2022) |> 
  summarise(
    countries = n_distinct(iso3c),
    pct = mean(obs_value),
    .by=c(insurance.type)) |> 
  arrange(desc(pct)) |> 
  gt(rowname_col=1, locale="pt") |> sub_missing() |>
  fmt_integer(c(countries)) |> fmt_number(pct, decimals=1) |> 
  gtExtras::gt_plt_bar(pct, keep_column=T) |> 
  tab_options(
    heading.align="left", heading.title.font.size=pct(110), heading.subtitle.font.size=pct(90),
    column_labels.font.weight="bold", column_labels.font.size=pct(80),
    column_labels.text_transform="uppercase", column_labels.background.color="gray95",
    data_row.padding=px(2), row_group.padding=px(2), row_group.font.weight="bold",
    table.font.size=pct(90), source_notes.font.size = pct(70)
  ) |> 
  tab_header(title=md("**Health Coverage by Insurance Type, 2022**"),
             subtitle="Percentage of population") |> 
  tab_source_note("Source: OECD, OECD Health Statistics 2023. July 2023.")
```

<div id="ltgvfwboig" style="padding-left:0px;padding-right:0px;padding-top:10px;padding-bottom:10px;overflow-x:auto;overflow-y:auto;width:auto;height:auto;">
<style>#ltgvfwboig table {
  font-family: system-ui, 'Segoe UI', Roboto, Helvetica, Arial, sans-serif, 'Apple Color Emoji', 'Segoe UI Emoji', 'Segoe UI Symbol', 'Noto Color Emoji';
  -webkit-font-smoothing: antialiased;
  -moz-osx-font-smoothing: grayscale;
}
&#10;#ltgvfwboig thead, #ltgvfwboig tbody, #ltgvfwboig tfoot, #ltgvfwboig tr, #ltgvfwboig td, #ltgvfwboig th {
  border-style: none;
}
&#10;#ltgvfwboig p {
  margin: 0;
  padding: 0;
}
&#10;#ltgvfwboig .gt_table {
  display: table;
  border-collapse: collapse;
  line-height: normal;
  margin-left: auto;
  margin-right: auto;
  color: #333333;
  font-size: 90%;
  font-weight: normal;
  font-style: normal;
  background-color: #FFFFFF;
  width: auto;
  border-top-style: solid;
  border-top-width: 2px;
  border-top-color: #A8A8A8;
  border-right-style: none;
  border-right-width: 2px;
  border-right-color: #D3D3D3;
  border-bottom-style: solid;
  border-bottom-width: 2px;
  border-bottom-color: #A8A8A8;
  border-left-style: none;
  border-left-width: 2px;
  border-left-color: #D3D3D3;
}
&#10;#ltgvfwboig .gt_caption {
  padding-top: 4px;
  padding-bottom: 4px;
}
&#10;#ltgvfwboig .gt_title {
  color: #333333;
  font-size: 110%;
  font-weight: initial;
  padding-top: 4px;
  padding-bottom: 4px;
  padding-left: 5px;
  padding-right: 5px;
  border-bottom-color: #FFFFFF;
  border-bottom-width: 0;
}
&#10;#ltgvfwboig .gt_subtitle {
  color: #333333;
  font-size: 90%;
  font-weight: initial;
  padding-top: 3px;
  padding-bottom: 5px;
  padding-left: 5px;
  padding-right: 5px;
  border-top-color: #FFFFFF;
  border-top-width: 0;
}
&#10;#ltgvfwboig .gt_heading {
  background-color: #FFFFFF;
  text-align: left;
  border-bottom-color: #FFFFFF;
  border-left-style: none;
  border-left-width: 1px;
  border-left-color: #D3D3D3;
  border-right-style: none;
  border-right-width: 1px;
  border-right-color: #D3D3D3;
}
&#10;#ltgvfwboig .gt_bottom_border {
  border-bottom-style: solid;
  border-bottom-width: 2px;
  border-bottom-color: #D3D3D3;
}
&#10;#ltgvfwboig .gt_col_headings {
  border-top-style: solid;
  border-top-width: 2px;
  border-top-color: #D3D3D3;
  border-bottom-style: solid;
  border-bottom-width: 2px;
  border-bottom-color: #D3D3D3;
  border-left-style: none;
  border-left-width: 1px;
  border-left-color: #D3D3D3;
  border-right-style: none;
  border-right-width: 1px;
  border-right-color: #D3D3D3;
}
&#10;#ltgvfwboig .gt_col_heading {
  color: #333333;
  background-color: #F2F2F2;
  font-size: 80%;
  font-weight: bold;
  text-transform: uppercase;
  border-left-style: none;
  border-left-width: 1px;
  border-left-color: #D3D3D3;
  border-right-style: none;
  border-right-width: 1px;
  border-right-color: #D3D3D3;
  vertical-align: bottom;
  padding-top: 5px;
  padding-bottom: 6px;
  padding-left: 5px;
  padding-right: 5px;
  overflow-x: hidden;
}
&#10;#ltgvfwboig .gt_column_spanner_outer {
  color: #333333;
  background-color: #F2F2F2;
  font-size: 80%;
  font-weight: bold;
  text-transform: uppercase;
  padding-top: 0;
  padding-bottom: 0;
  padding-left: 4px;
  padding-right: 4px;
}
&#10;#ltgvfwboig .gt_column_spanner_outer:first-child {
  padding-left: 0;
}
&#10;#ltgvfwboig .gt_column_spanner_outer:last-child {
  padding-right: 0;
}
&#10;#ltgvfwboig .gt_column_spanner {
  border-bottom-style: solid;
  border-bottom-width: 2px;
  border-bottom-color: #D3D3D3;
  vertical-align: bottom;
  padding-top: 5px;
  padding-bottom: 5px;
  overflow-x: hidden;
  display: inline-block;
  width: 100%;
}
&#10;#ltgvfwboig .gt_spanner_row {
  border-bottom-style: hidden;
}
&#10;#ltgvfwboig .gt_group_heading {
  padding-top: 2px;
  padding-bottom: 2px;
  padding-left: 5px;
  padding-right: 5px;
  color: #333333;
  background-color: #FFFFFF;
  font-size: 100%;
  font-weight: bold;
  text-transform: inherit;
  border-top-style: solid;
  border-top-width: 2px;
  border-top-color: #D3D3D3;
  border-bottom-style: solid;
  border-bottom-width: 2px;
  border-bottom-color: #D3D3D3;
  border-left-style: none;
  border-left-width: 1px;
  border-left-color: #D3D3D3;
  border-right-style: none;
  border-right-width: 1px;
  border-right-color: #D3D3D3;
  vertical-align: middle;
  text-align: left;
}
&#10;#ltgvfwboig .gt_empty_group_heading {
  padding: 0.5px;
  color: #333333;
  background-color: #FFFFFF;
  font-size: 100%;
  font-weight: bold;
  border-top-style: solid;
  border-top-width: 2px;
  border-top-color: #D3D3D3;
  border-bottom-style: solid;
  border-bottom-width: 2px;
  border-bottom-color: #D3D3D3;
  vertical-align: middle;
}
&#10;#ltgvfwboig .gt_from_md > :first-child {
  margin-top: 0;
}
&#10;#ltgvfwboig .gt_from_md > :last-child {
  margin-bottom: 0;
}
&#10;#ltgvfwboig .gt_row {
  padding-top: 2px;
  padding-bottom: 2px;
  padding-left: 5px;
  padding-right: 5px;
  margin: 10px;
  border-top-style: solid;
  border-top-width: 1px;
  border-top-color: #D3D3D3;
  border-left-style: none;
  border-left-width: 1px;
  border-left-color: #D3D3D3;
  border-right-style: none;
  border-right-width: 1px;
  border-right-color: #D3D3D3;
  vertical-align: middle;
  overflow-x: hidden;
}
&#10;#ltgvfwboig .gt_stub {
  color: #333333;
  background-color: #FFFFFF;
  font-size: 100%;
  font-weight: initial;
  text-transform: inherit;
  border-right-style: solid;
  border-right-width: 2px;
  border-right-color: #D3D3D3;
  padding-left: 5px;
  padding-right: 5px;
}
&#10;#ltgvfwboig .gt_stub_row_group {
  color: #333333;
  background-color: #FFFFFF;
  font-size: 100%;
  font-weight: initial;
  text-transform: inherit;
  border-right-style: solid;
  border-right-width: 2px;
  border-right-color: #D3D3D3;
  padding-left: 5px;
  padding-right: 5px;
  vertical-align: top;
}
&#10;#ltgvfwboig .gt_row_group_first td {
  border-top-width: 2px;
}
&#10;#ltgvfwboig .gt_row_group_first th {
  border-top-width: 2px;
}
&#10;#ltgvfwboig .gt_summary_row {
  color: #333333;
  background-color: #FFFFFF;
  text-transform: inherit;
  padding-top: 8px;
  padding-bottom: 8px;
  padding-left: 5px;
  padding-right: 5px;
}
&#10;#ltgvfwboig .gt_first_summary_row {
  border-top-style: solid;
  border-top-color: #D3D3D3;
}
&#10;#ltgvfwboig .gt_first_summary_row.thick {
  border-top-width: 2px;
}
&#10;#ltgvfwboig .gt_last_summary_row {
  padding-top: 8px;
  padding-bottom: 8px;
  padding-left: 5px;
  padding-right: 5px;
  border-bottom-style: solid;
  border-bottom-width: 2px;
  border-bottom-color: #D3D3D3;
}
&#10;#ltgvfwboig .gt_grand_summary_row {
  color: #333333;
  background-color: #FFFFFF;
  text-transform: inherit;
  padding-top: 8px;
  padding-bottom: 8px;
  padding-left: 5px;
  padding-right: 5px;
}
&#10;#ltgvfwboig .gt_first_grand_summary_row {
  padding-top: 8px;
  padding-bottom: 8px;
  padding-left: 5px;
  padding-right: 5px;
  border-top-style: double;
  border-top-width: 6px;
  border-top-color: #D3D3D3;
}
&#10;#ltgvfwboig .gt_last_grand_summary_row_top {
  padding-top: 8px;
  padding-bottom: 8px;
  padding-left: 5px;
  padding-right: 5px;
  border-bottom-style: double;
  border-bottom-width: 6px;
  border-bottom-color: #D3D3D3;
}
&#10;#ltgvfwboig .gt_striped {
  background-color: rgba(128, 128, 128, 0.05);
}
&#10;#ltgvfwboig .gt_table_body {
  border-top-style: solid;
  border-top-width: 2px;
  border-top-color: #D3D3D3;
  border-bottom-style: solid;
  border-bottom-width: 2px;
  border-bottom-color: #D3D3D3;
}
&#10;#ltgvfwboig .gt_footnotes {
  color: #333333;
  background-color: #FFFFFF;
  border-bottom-style: none;
  border-bottom-width: 2px;
  border-bottom-color: #D3D3D3;
  border-left-style: none;
  border-left-width: 2px;
  border-left-color: #D3D3D3;
  border-right-style: none;
  border-right-width: 2px;
  border-right-color: #D3D3D3;
}
&#10;#ltgvfwboig .gt_footnote {
  margin: 0px;
  font-size: 90%;
  padding-top: 4px;
  padding-bottom: 4px;
  padding-left: 5px;
  padding-right: 5px;
}
&#10;#ltgvfwboig .gt_sourcenotes {
  color: #333333;
  background-color: #FFFFFF;
  border-bottom-style: none;
  border-bottom-width: 2px;
  border-bottom-color: #D3D3D3;
  border-left-style: none;
  border-left-width: 2px;
  border-left-color: #D3D3D3;
  border-right-style: none;
  border-right-width: 2px;
  border-right-color: #D3D3D3;
}
&#10;#ltgvfwboig .gt_sourcenote {
  font-size: 70%;
  padding-top: 4px;
  padding-bottom: 4px;
  padding-left: 5px;
  padding-right: 5px;
}
&#10;#ltgvfwboig .gt_left {
  text-align: left;
}
&#10;#ltgvfwboig .gt_center {
  text-align: center;
}
&#10;#ltgvfwboig .gt_right {
  text-align: right;
  font-variant-numeric: tabular-nums;
}
&#10;#ltgvfwboig .gt_font_normal {
  font-weight: normal;
}
&#10;#ltgvfwboig .gt_font_bold {
  font-weight: bold;
}
&#10;#ltgvfwboig .gt_font_italic {
  font-style: italic;
}
&#10;#ltgvfwboig .gt_super {
  font-size: 65%;
}
&#10;#ltgvfwboig .gt_footnote_marks {
  font-size: 75%;
  vertical-align: 0.4em;
  position: initial;
}
&#10;#ltgvfwboig .gt_asterisk {
  font-size: 100%;
  vertical-align: 0;
}
&#10;#ltgvfwboig .gt_indent_1 {
  text-indent: 5px;
}
&#10;#ltgvfwboig .gt_indent_2 {
  text-indent: 10px;
}
&#10;#ltgvfwboig .gt_indent_3 {
  text-indent: 15px;
}
&#10;#ltgvfwboig .gt_indent_4 {
  text-indent: 20px;
}
&#10;#ltgvfwboig .gt_indent_5 {
  text-indent: 25px;
}
</style>
<table class="gt_table" data-quarto-disable-processing="false" data-quarto-bootstrap="false">
  <thead>
    <tr class="gt_heading">
      <td colspan="4" class="gt_heading gt_title gt_font_normal" style><strong>Health Coverage by Insurance Type, 2022</strong></td>
    </tr>
    <tr class="gt_heading">
      <td colspan="4" class="gt_heading gt_subtitle gt_font_normal gt_bottom_border" style>Percentage of population</td>
    </tr>
    <tr class="gt_col_headings">
      <th class="gt_col_heading gt_columns_bottom_border gt_left" rowspan="1" colspan="1" scope="col" id="insurance.type">insurance.type</th>
      <th class="gt_col_heading gt_columns_bottom_border gt_right" rowspan="1" colspan="1" scope="col" id="countries">countries</th>
      <th class="gt_col_heading gt_columns_bottom_border gt_right" rowspan="1" colspan="1" scope="col" id="pct">pct</th>
      <th class="gt_col_heading gt_columns_bottom_border gt_left" rowspan="1" colspan="1" scope="col" id="pct">pct</th>
    </tr>
  </thead>
  <tbody class="gt_table_body">
    <tr><td headers="insurance.type" class="gt_row gt_left">Public and primary voluntary health insurance</td>
<td headers="countries" class="gt_row gt_right">20</td>
<td headers="pct" class="gt_row gt_right">98,7</td>
<td headers="DUPE_COLUMN_PLT" class="gt_row gt_left"><?xml version='1.0' encoding='UTF-8' ?><svg xmlns='http://www.w3.org/2000/svg' xmlns:xlink='http://www.w3.org/1999/xlink' class='svglite' width='113.39pt' height='14.17pt' viewBox='0 0 113.39 14.17'><defs>  <style type='text/css'><![CDATA[    .svglite line, .svglite polyline, .svglite polygon, .svglite path, .svglite rect, .svglite circle {      fill: none;      stroke: #000000;      stroke-linecap: round;      stroke-linejoin: round;      stroke-miterlimit: 10.00;    }    .svglite text {      white-space: pre;    }  ]]></style></defs><rect width='100%' height='100%' style='stroke: none; fill: none;'/><defs>  <clipPath id='cpMC4wMHwxMTMuMzl8MC4wMHwxNC4xNw=='>    <rect x='0.00' y='0.00' width='113.39' height='14.17' />  </clipPath></defs><g clip-path='url(#cpMC4wMHwxMTMuMzl8MC4wMHwxNC4xNw==)'><rect x='5.02' y='0.89' width='98.37' height='12.40' style='stroke-width: 1.07; stroke: none; stroke-linecap: butt; stroke-linejoin: miter; fill: #A020F0;' /><line x1='5.02' y1='14.17' x2='5.02' y2='0.0000000000000018' style='stroke-width: 1.07; stroke-linecap: butt;' /></g></svg></td></tr>
    <tr><td headers="insurance.type" class="gt_row gt_left">Government/compulsory health insurance</td>
<td headers="countries" class="gt_row gt_right">20</td>
<td headers="pct" class="gt_row gt_right">97,9</td>
<td headers="DUPE_COLUMN_PLT" class="gt_row gt_left"><?xml version='1.0' encoding='UTF-8' ?><svg xmlns='http://www.w3.org/2000/svg' xmlns:xlink='http://www.w3.org/1999/xlink' class='svglite' width='113.39pt' height='14.17pt' viewBox='0 0 113.39 14.17'><defs>  <style type='text/css'><![CDATA[    .svglite line, .svglite polyline, .svglite polygon, .svglite path, .svglite rect, .svglite circle {      fill: none;      stroke: #000000;      stroke-linecap: round;      stroke-linejoin: round;      stroke-miterlimit: 10.00;    }    .svglite text {      white-space: pre;    }  ]]></style></defs><rect width='100%' height='100%' style='stroke: none; fill: none;'/><defs>  <clipPath id='cpMC4wMHwxMTMuMzl8MC4wMHwxNC4xNw=='>    <rect x='0.00' y='0.00' width='113.39' height='14.17' />  </clipPath></defs><g clip-path='url(#cpMC4wMHwxMTMuMzl8MC4wMHwxNC4xNw==)'><rect x='5.02' y='0.89' width='97.58' height='12.40' style='stroke-width: 1.07; stroke: none; stroke-linecap: butt; stroke-linejoin: miter; fill: #A020F0;' /><line x1='5.02' y1='14.17' x2='5.02' y2='0.0000000000000018' style='stroke-width: 1.07; stroke-linecap: butt;' /></g></svg></td></tr>
    <tr><td headers="insurance.type" class="gt_row gt_left">Total voluntary health insurance</td>
<td headers="countries" class="gt_row gt_right">10</td>
<td headers="pct" class="gt_row gt_right">34,0</td>
<td headers="DUPE_COLUMN_PLT" class="gt_row gt_left"><?xml version='1.0' encoding='UTF-8' ?><svg xmlns='http://www.w3.org/2000/svg' xmlns:xlink='http://www.w3.org/1999/xlink' class='svglite' width='113.39pt' height='14.17pt' viewBox='0 0 113.39 14.17'><defs>  <style type='text/css'><![CDATA[    .svglite line, .svglite polyline, .svglite polygon, .svglite path, .svglite rect, .svglite circle {      fill: none;      stroke: #000000;      stroke-linecap: round;      stroke-linejoin: round;      stroke-miterlimit: 10.00;    }    .svglite text {      white-space: pre;    }  ]]></style></defs><rect width='100%' height='100%' style='stroke: none; fill: none;'/><defs>  <clipPath id='cpMC4wMHwxMTMuMzl8MC4wMHwxNC4xNw=='>    <rect x='0.00' y='0.00' width='113.39' height='14.17' />  </clipPath></defs><g clip-path='url(#cpMC4wMHwxMTMuMzl8MC4wMHwxNC4xNw==)'><rect x='5.02' y='0.89' width='33.90' height='12.40' style='stroke-width: 1.07; stroke: none; stroke-linecap: butt; stroke-linejoin: miter; fill: #A020F0;' /><line x1='5.02' y1='14.17' x2='5.02' y2='0.0000000000000018' style='stroke-width: 1.07; stroke-linecap: butt;' /></g></svg></td></tr>
    <tr><td headers="insurance.type" class="gt_row gt_left">Supplementary voluntary health insurance</td>
<td headers="countries" class="gt_row gt_right">7</td>
<td headers="pct" class="gt_row gt_right">31,4</td>
<td headers="DUPE_COLUMN_PLT" class="gt_row gt_left"><?xml version='1.0' encoding='UTF-8' ?><svg xmlns='http://www.w3.org/2000/svg' xmlns:xlink='http://www.w3.org/1999/xlink' class='svglite' width='113.39pt' height='14.17pt' viewBox='0 0 113.39 14.17'><defs>  <style type='text/css'><![CDATA[    .svglite line, .svglite polyline, .svglite polygon, .svglite path, .svglite rect, .svglite circle {      fill: none;      stroke: #000000;      stroke-linecap: round;      stroke-linejoin: round;      stroke-miterlimit: 10.00;    }    .svglite text {      white-space: pre;    }  ]]></style></defs><rect width='100%' height='100%' style='stroke: none; fill: none;'/><defs>  <clipPath id='cpMC4wMHwxMTMuMzl8MC4wMHwxNC4xNw=='>    <rect x='0.00' y='0.00' width='113.39' height='14.17' />  </clipPath></defs><g clip-path='url(#cpMC4wMHwxMTMuMzl8MC4wMHwxNC4xNw==)'><rect x='5.02' y='0.89' width='31.31' height='12.40' style='stroke-width: 1.07; stroke: none; stroke-linecap: butt; stroke-linejoin: miter; fill: #A020F0;' /><line x1='5.02' y1='14.17' x2='5.02' y2='0.0000000000000018' style='stroke-width: 1.07; stroke-linecap: butt;' /></g></svg></td></tr>
    <tr><td headers="insurance.type" class="gt_row gt_left">Complementary voluntary health insurance</td>
<td headers="countries" class="gt_row gt_right">7</td>
<td headers="pct" class="gt_row gt_right">30,2</td>
<td headers="DUPE_COLUMN_PLT" class="gt_row gt_left"><?xml version='1.0' encoding='UTF-8' ?><svg xmlns='http://www.w3.org/2000/svg' xmlns:xlink='http://www.w3.org/1999/xlink' class='svglite' width='113.39pt' height='14.17pt' viewBox='0 0 113.39 14.17'><defs>  <style type='text/css'><![CDATA[    .svglite line, .svglite polyline, .svglite polygon, .svglite path, .svglite rect, .svglite circle {      fill: none;      stroke: #000000;      stroke-linecap: round;      stroke-linejoin: round;      stroke-miterlimit: 10.00;    }    .svglite text {      white-space: pre;    }  ]]></style></defs><rect width='100%' height='100%' style='stroke: none; fill: none;'/><defs>  <clipPath id='cpMC4wMHwxMTMuMzl8MC4wMHwxNC4xNw=='>    <rect x='0.00' y='0.00' width='113.39' height='14.17' />  </clipPath></defs><g clip-path='url(#cpMC4wMHwxMTMuMzl8MC4wMHwxNC4xNw==)'><rect x='5.02' y='0.89' width='30.07' height='12.40' style='stroke-width: 1.07; stroke: none; stroke-linecap: butt; stroke-linejoin: miter; fill: #A020F0;' /><line x1='5.02' y1='14.17' x2='5.02' y2='0.0000000000000018' style='stroke-width: 1.07; stroke-linecap: butt;' /></g></svg></td></tr>
    <tr><td headers="insurance.type" class="gt_row gt_left">Duplicate voluntary health insurance</td>
<td headers="countries" class="gt_row gt_right">4</td>
<td headers="pct" class="gt_row gt_right">24,6</td>
<td headers="DUPE_COLUMN_PLT" class="gt_row gt_left"><?xml version='1.0' encoding='UTF-8' ?><svg xmlns='http://www.w3.org/2000/svg' xmlns:xlink='http://www.w3.org/1999/xlink' class='svglite' width='113.39pt' height='14.17pt' viewBox='0 0 113.39 14.17'><defs>  <style type='text/css'><![CDATA[    .svglite line, .svglite polyline, .svglite polygon, .svglite path, .svglite rect, .svglite circle {      fill: none;      stroke: #000000;      stroke-linecap: round;      stroke-linejoin: round;      stroke-miterlimit: 10.00;    }    .svglite text {      white-space: pre;    }  ]]></style></defs><rect width='100%' height='100%' style='stroke: none; fill: none;'/><defs>  <clipPath id='cpMC4wMHwxMTMuMzl8MC4wMHwxNC4xNw=='>    <rect x='0.00' y='0.00' width='113.39' height='14.17' />  </clipPath></defs><g clip-path='url(#cpMC4wMHwxMTMuMzl8MC4wMHwxNC4xNw==)'><rect x='5.02' y='0.89' width='24.53' height='12.40' style='stroke-width: 1.07; stroke: none; stroke-linecap: butt; stroke-linejoin: miter; fill: #A020F0;' /><line x1='5.02' y1='14.17' x2='5.02' y2='0.0000000000000018' style='stroke-width: 1.07; stroke-linecap: butt;' /></g></svg></td></tr>
    <tr><td headers="insurance.type" class="gt_row gt_left">Primary voluntary health insurance</td>
<td headers="countries" class="gt_row gt_right">7</td>
<td headers="pct" class="gt_row gt_right">2,3</td>
<td headers="DUPE_COLUMN_PLT" class="gt_row gt_left"><?xml version='1.0' encoding='UTF-8' ?><svg xmlns='http://www.w3.org/2000/svg' xmlns:xlink='http://www.w3.org/1999/xlink' class='svglite' width='113.39pt' height='14.17pt' viewBox='0 0 113.39 14.17'><defs>  <style type='text/css'><![CDATA[    .svglite line, .svglite polyline, .svglite polygon, .svglite path, .svglite rect, .svglite circle {      fill: none;      stroke: #000000;      stroke-linecap: round;      stroke-linejoin: round;      stroke-miterlimit: 10.00;    }    .svglite text {      white-space: pre;    }  ]]></style></defs><rect width='100%' height='100%' style='stroke: none; fill: none;'/><defs>  <clipPath id='cpMC4wMHwxMTMuMzl8MC4wMHwxNC4xNw=='>    <rect x='0.00' y='0.00' width='113.39' height='14.17' />  </clipPath></defs><g clip-path='url(#cpMC4wMHwxMTMuMzl8MC4wMHwxNC4xNw==)'><rect x='5.02' y='0.89' width='2.26' height='12.40' style='stroke-width: 1.07; stroke: none; stroke-linecap: butt; stroke-linejoin: miter; fill: #A020F0;' /><line x1='5.02' y1='14.17' x2='5.02' y2='0.0000000000000018' style='stroke-width: 1.07; stroke-linecap: butt;' /></g></svg></td></tr>
  </tbody>
  <tfoot class="gt_sourcenotes">
    <tr>
      <td class="gt_sourcenote" colspan="4">Source: OECD, OECD Health Statistics 2023. July 2023.</td>
    </tr>
  </tfoot>
  &#10;</table>
</div>

Must add population data to get weighted average of coverage by
insurance type.
