//! layout tests for scenarios with large size overlapped files
//!
//! See [crate::layout] module for detailed documentation

use std::sync::Arc;

use data_types::CompactionLevel;
use iox_time::{MockProvider, Time, TimeProvider};

use crate::layouts::{layout_setup_builder, parquet_builder, run_layout_scenario, ONE_MB};

#[tokio::test]
async fn one_l1_overlaps_with_many_l2s() {
    // Simulate a production scenario in which there are two L1 files but one overlaps with three L2 files
    // and their total size > limit 256MB
    // |----------L2.1----------||----------L2.2----------||-----L2.3----|
    //  |----------------------------------------L1.1---------------------------||--L1.2--|

    test_helpers::maybe_start_logging();

    let setup = layout_setup_builder().await.build().await;
    let time_provider = Arc::new(MockProvider::new(Time::from_timestamp(0, 0).unwrap()));

    // L2: 100MB, 100MB, 70MB
    // L1: 100MB, 30MB

    // non-overlapping L2 files
    let mut num_l2_files = 0;
    for (i, sz) in [100, 100, 70].iter().enumerate() {
        let i = i as i64;
        setup
            .partition
            .create_parquet_file(
                parquet_builder()
                    .with_min_time(51 + i * 50)
                    .with_max_time(100 + i * 50)
                    .with_compaction_level(CompactionLevel::Final)
                    .with_file_size_bytes(sz * ONE_MB)
                    .with_max_l0_created_at(time_provider.minutes_into_future(i as u64)),
            )
            .await;
        num_l2_files += 1;
    }

    let l2_max_time: i64 = 100 + (num_l2_files - 1) * 50;
    // Non-overlapping L1 files but the first L1 file overlaps with all L2 files
    for (i, sz) in [100, 30].iter().enumerate() {
        setup
            .partition
            .create_parquet_file(
                parquet_builder()
                    .with_min_time(1 + i as i64 * (l2_max_time + 50))
                    .with_max_time((i as i64 + 1) * (l2_max_time + 50))
                    .with_compaction_level(CompactionLevel::FileNonOverlapped)
                    .with_file_size_bytes(sz * ONE_MB)
                    .with_max_l0_created_at(
                        time_provider.minutes_into_future(num_l2_files as u64 + i as u64 + 1),
                    ),
            )
            .await;
    }

    insta::assert_yaml_snapshot!(
        run_layout_scenario(&setup).await,
        @r###"
    ---
    - "**** Input Files "
    - "L1                                                                                                  "
    - "L1.4[1,250] 100mb   |----------------L1.4-----------------|                                         "
    - "L1.5[251,500] 30mb                                          |----------------L1.5-----------------| "
    - "L2                                                                                                  "
    - "L2.1[51,100] 100mb          |L2.1-|                                                                 "
    - "L2.2[101,150] 100mb                 |L2.2-|                                                         "
    - "L2.3[151,200] 70mb                          |L2.3-|                                                 "
    - "SKIPPED COMPACTION for PartitionId(1): partition 1 has 419430400 parquet file bytes, limit is 268435456"
    - "**** Final Output Files "
    - "L1                                                                                                  "
    - "L1.4[1,250] 100mb   |----------------L1.4-----------------|                                         "
    - "L1.5[251,500] 30mb                                          |----------------L1.5-----------------| "
    - "L2                                                                                                  "
    - "L2.1[51,100] 100mb          |L2.1-|                                                                 "
    - "L2.2[101,150] 100mb                 |L2.2-|                                                         "
    - "L2.3[151,200] 70mb                          |L2.3-|                                                 "
    "###
    );
}

#[tokio::test]
async fn many_l1_overlaps_with_many_l2s() {
    // Simulate a production scenario in which many L1 files overlap with three L2 files
    // and their total size > limit 256MB
    // |------------L2.1------------||------------L2.2------------||-----L2.3----|
    // |---L1.4---||---L1.5---||-L1.6-||-L1.7-||--L1.8--||--L1.9--||--L1.10--|                      |--L1.11--|

    test_helpers::maybe_start_logging();

    let setup = layout_setup_builder().await.build().await;
    let time_provider = Arc::new(MockProvider::new(Time::from_timestamp(0, 0).unwrap()));

    // L2: 100MB, 100MB, 70MB
    // L1: 8 files each 13MB to have their total size > 100MB to force the compactor to compact them with overlapped L2 files

    // non-overlapping L2 files
    let mut num_l2_files = 0;
    for (i, sz) in [100, 100, 70].iter().enumerate() {
        let i = i as i64;
        setup
            .partition
            .create_parquet_file(
                parquet_builder()
                    .with_min_time(51 + i * 50)
                    .with_max_time(100 + i * 50)
                    .with_compaction_level(CompactionLevel::Final)
                    .with_file_size_bytes(sz * ONE_MB)
                    .with_max_l0_created_at(time_provider.minutes_into_future(i as u64)),
            )
            .await;
        num_l2_files += 1;
    }

    // Non-overlapping L1 files but they overlap with L2 files as in the diagram above
    let mut num_l1_files = 0;
    for (i, sz) in [13, 13, 13, 13, 13, 13, 13].iter().enumerate() {
        setup
            .partition
            .create_parquet_file(
                parquet_builder()
                    .with_min_time(61 + i as i64 * 15)
                    .with_max_time(75 + i as i64 * 15)
                    .with_compaction_level(CompactionLevel::FileNonOverlapped)
                    .with_file_size_bytes(sz * ONE_MB)
                    .with_max_l0_created_at(
                        time_provider.minutes_into_future(num_l2_files as u64 + i as u64 + 1),
                    ),
            )
            .await;
        num_l1_files += 1;
    }
    // Have the last L1 not overlap with anything
    setup
        .partition
        .create_parquet_file(
            parquet_builder()
                .with_min_time((num_l2_files as i64 + 1) * 50 + 1)
                .with_max_time((num_l2_files as i64 + 1) * 50 + 15)
                .with_compaction_level(CompactionLevel::FileNonOverlapped)
                .with_file_size_bytes(13 * ONE_MB)
                .with_max_l0_created_at(
                    time_provider
                        .minutes_into_future(num_l2_files as u64 + num_l1_files as u64 + 1),
                ),
        )
        .await;

    insta::assert_yaml_snapshot!(
        run_layout_scenario(&setup).await,
        @r###"
    ---
    - "**** Input Files "
    - "L1                                                                                                  "
    - "L1.4[61,75] 13mb        |L1.4|                                                                      "
    - "L1.5[76,90] 13mb                |L1.5|                                                              "
    - "L1.6[91,105] 13mb                      |L1.6|                                                       "
    - "L1.7[106,120] 13mb                            |L1.7|                                                "
    - "L1.8[121,135] 13mb                                    |L1.8|                                        "
    - "L1.9[136,150] 13mb                                           |L1.9|                                 "
    - "L1.10[151,165] 13mb                                                 |L1.10|                         "
    - "L1.11[201,215] 13mb                                                                          |L1.11|"
    - "L2                                                                                                  "
    - "L2.1[51,100] 100mb  |--------L2.1---------|                                                         "
    - "L2.2[101,150] 100mb                         |--------L2.2---------|                                 "
    - "L2.3[151,200] 70mb                                                  |--------L2.3---------|         "
    - "SKIPPED COMPACTION for PartitionId(1): partition 1 has 392167424 parquet file bytes, limit is 268435456"
    - "**** Final Output Files "
    - "L1                                                                                                  "
    - "L1.4[61,75] 13mb        |L1.4|                                                                      "
    - "L1.5[76,90] 13mb                |L1.5|                                                              "
    - "L1.6[91,105] 13mb                      |L1.6|                                                       "
    - "L1.7[106,120] 13mb                            |L1.7|                                                "
    - "L1.8[121,135] 13mb                                    |L1.8|                                        "
    - "L1.9[136,150] 13mb                                           |L1.9|                                 "
    - "L1.10[151,165] 13mb                                                 |L1.10|                         "
    - "L1.11[201,215] 13mb                                                                          |L1.11|"
    - "L2                                                                                                  "
    - "L2.1[51,100] 100mb  |--------L2.1---------|                                                         "
    - "L2.2[101,150] 100mb                         |--------L2.2---------|                                 "
    - "L2.3[151,200] 70mb                                                  |--------L2.3---------|         "
    "###
    );
}

#[tokio::test]
async fn many_good_size_l0_files() {
    test_helpers::maybe_start_logging();

    // Scenario when we have a lot of L0 files becasue the compactor cannot keep up with ingesters
    // and each files is a good size, 2MB, which lead to total size of 200 files (limit num files)
    // greater than limit size (256MB)

    let two_mb = 2 * ONE_MB;

    let setup = layout_setup_builder().await.build().await;

    let num_files = (24 * 60) / 5; // 288 files

    for i in 0..num_files {
        let i = i as i64;
        setup
            .partition
            .create_parquet_file(
                parquet_builder()
                    .with_min_time(i)
                    .with_max_time(i + 1)
                    .with_compaction_level(CompactionLevel::Initial)
                    .with_file_size_bytes(two_mb),
            )
            .await;
    }

    insta::assert_yaml_snapshot!(
        run_layout_scenario(&setup).await,
        @r###"
    ---
    - "**** Input Files "
    - "L0, all files 2mb                                                                                   "
    - "L0.1[0,1]           |L0.1|                                                                          "
    - "L0.2[1,2]           |L0.2|                                                                          "
    - "L0.3[2,3]           |L0.3|                                                                          "
    - "L0.4[3,4]           |L0.4|                                                                          "
    - "L0.5[4,5]            |L0.5|                                                                         "
    - "L0.6[5,6]            |L0.6|                                                                         "
    - "L0.7[6,7]            |L0.7|                                                                         "
    - "L0.8[7,8]            |L0.8|                                                                         "
    - "L0.9[8,9]             |L0.9|                                                                        "
    - "L0.10[9,10]           |L0.10|                                                                       "
    - "L0.11[10,11]          |L0.11|                                                                       "
    - "L0.12[11,12]           |L0.12|                                                                      "
    - "L0.13[12,13]           |L0.13|                                                                      "
    - "L0.14[13,14]           |L0.14|                                                                      "
    - "L0.15[14,15]           |L0.15|                                                                      "
    - "L0.16[15,16]            |L0.16|                                                                     "
    - "L0.17[16,17]            |L0.17|                                                                     "
    - "L0.18[17,18]            |L0.18|                                                                     "
    - "L0.19[18,19]             |L0.19|                                                                    "
    - "L0.20[19,20]             |L0.20|                                                                    "
    - "L0.21[20,21]             |L0.21|                                                                    "
    - "L0.22[21,22]             |L0.22|                                                                    "
    - "L0.23[22,23]              |L0.23|                                                                   "
    - "L0.24[23,24]              |L0.24|                                                                   "
    - "L0.25[24,25]              |L0.25|                                                                   "
    - "L0.26[25,26]              |L0.26|                                                                   "
    - "L0.27[26,27]               |L0.27|                                                                  "
    - "L0.28[27,28]               |L0.28|                                                                  "
    - "L0.29[28,29]               |L0.29|                                                                  "
    - "L0.30[29,30]                |L0.30|                                                                 "
    - "L0.31[30,31]                |L0.31|                                                                 "
    - "L0.32[31,32]                |L0.32|                                                                 "
    - "L0.33[32,33]                |L0.33|                                                                 "
    - "L0.34[33,34]                 |L0.34|                                                                "
    - "L0.35[34,35]                 |L0.35|                                                                "
    - "L0.36[35,36]                 |L0.36|                                                                "
    - "L0.37[36,37]                  |L0.37|                                                               "
    - "L0.38[37,38]                  |L0.38|                                                               "
    - "L0.39[38,39]                  |L0.39|                                                               "
    - "L0.40[39,40]                  |L0.40|                                                               "
    - "L0.41[40,41]                   |L0.41|                                                              "
    - "L0.42[41,42]                   |L0.42|                                                              "
    - "L0.43[42,43]                   |L0.43|                                                              "
    - "L0.44[43,44]                   |L0.44|                                                              "
    - "L0.45[44,45]                    |L0.45|                                                             "
    - "L0.46[45,46]                    |L0.46|                                                             "
    - "L0.47[46,47]                    |L0.47|                                                             "
    - "L0.48[47,48]                     |L0.48|                                                            "
    - "L0.49[48,49]                     |L0.49|                                                            "
    - "L0.50[49,50]                     |L0.50|                                                            "
    - "L0.51[50,51]                     |L0.51|                                                            "
    - "L0.52[51,52]                      |L0.52|                                                           "
    - "L0.53[52,53]                      |L0.53|                                                           "
    - "L0.54[53,54]                      |L0.54|                                                           "
    - "L0.55[54,55]                       |L0.55|                                                          "
    - "L0.56[55,56]                       |L0.56|                                                          "
    - "L0.57[56,57]                       |L0.57|                                                          "
    - "L0.58[57,58]                       |L0.58|                                                          "
    - "L0.59[58,59]                        |L0.59|                                                         "
    - "L0.60[59,60]                        |L0.60|                                                         "
    - "L0.61[60,61]                        |L0.61|                                                         "
    - "L0.62[61,62]                        |L0.62|                                                         "
    - "L0.63[62,63]                         |L0.63|                                                        "
    - "L0.64[63,64]                         |L0.64|                                                        "
    - "L0.65[64,65]                         |L0.65|                                                        "
    - "L0.66[65,66]                          |L0.66|                                                       "
    - "L0.67[66,67]                          |L0.67|                                                       "
    - "L0.68[67,68]                          |L0.68|                                                       "
    - "L0.69[68,69]                          |L0.69|                                                       "
    - "L0.70[69,70]                           |L0.70|                                                      "
    - "L0.71[70,71]                           |L0.71|                                                      "
    - "L0.72[71,72]                           |L0.72|                                                      "
    - "L0.73[72,73]                            |L0.73|                                                     "
    - "L0.74[73,74]                            |L0.74|                                                     "
    - "L0.75[74,75]                            |L0.75|                                                     "
    - "L0.76[75,76]                            |L0.76|                                                     "
    - "L0.77[76,77]                             |L0.77|                                                    "
    - "L0.78[77,78]                             |L0.78|                                                    "
    - "L0.79[78,79]                             |L0.79|                                                    "
    - "L0.80[79,80]                             |L0.80|                                                    "
    - "L0.81[80,81]                              |L0.81|                                                   "
    - "L0.82[81,82]                              |L0.82|                                                   "
    - "L0.83[82,83]                              |L0.83|                                                   "
    - "L0.84[83,84]                               |L0.84|                                                  "
    - "L0.85[84,85]                               |L0.85|                                                  "
    - "L0.86[85,86]                               |L0.86|                                                  "
    - "L0.87[86,87]                               |L0.87|                                                  "
    - "L0.88[87,88]                                |L0.88|                                                 "
    - "L0.89[88,89]                                |L0.89|                                                 "
    - "L0.90[89,90]                                |L0.90|                                                 "
    - "L0.91[90,91]                                 |L0.91|                                                "
    - "L0.92[91,92]                                 |L0.92|                                                "
    - "L0.93[92,93]                                 |L0.93|                                                "
    - "L0.94[93,94]                                 |L0.94|                                                "
    - "L0.95[94,95]                                  |L0.95|                                               "
    - "L0.96[95,96]                                  |L0.96|                                               "
    - "L0.97[96,97]                                  |L0.97|                                               "
    - "L0.98[97,98]                                  |L0.98|                                               "
    - "L0.99[98,99]                                   |L0.99|                                              "
    - "L0.100[99,100]                                 |L0.100|                                             "
    - "L0.101[100,101]                                |L0.101|                                             "
    - "L0.102[101,102]                                 |L0.102|                                            "
    - "L0.103[102,103]                                 |L0.103|                                            "
    - "L0.104[103,104]                                 |L0.104|                                            "
    - "L0.105[104,105]                                 |L0.105|                                            "
    - "L0.106[105,106]                                  |L0.106|                                           "
    - "L0.107[106,107]                                  |L0.107|                                           "
    - "L0.108[107,108]                                  |L0.108|                                           "
    - "L0.109[108,109]                                   |L0.109|                                          "
    - "L0.110[109,110]                                   |L0.110|                                          "
    - "L0.111[110,111]                                   |L0.111|                                          "
    - "L0.112[111,112]                                   |L0.112|                                          "
    - "L0.113[112,113]                                    |L0.113|                                         "
    - "L0.114[113,114]                                    |L0.114|                                         "
    - "L0.115[114,115]                                    |L0.115|                                         "
    - "L0.116[115,116]                                    |L0.116|                                         "
    - "L0.117[116,117]                                     |L0.117|                                        "
    - "L0.118[117,118]                                     |L0.118|                                        "
    - "L0.119[118,119]                                     |L0.119|                                        "
    - "L0.120[119,120]                                      |L0.120|                                       "
    - "L0.121[120,121]                                      |L0.121|                                       "
    - "L0.122[121,122]                                      |L0.122|                                       "
    - "L0.123[122,123]                                      |L0.123|                                       "
    - "L0.124[123,124]                                       |L0.124|                                      "
    - "L0.125[124,125]                                       |L0.125|                                      "
    - "L0.126[125,126]                                       |L0.126|                                      "
    - "L0.127[126,127]                                        |L0.127|                                     "
    - "L0.128[127,128]                                        |L0.128|                                     "
    - "L0.129[128,129]                                        |L0.129|                                     "
    - "L0.130[129,130]                                        |L0.130|                                     "
    - "L0.131[130,131]                                         |L0.131|                                    "
    - "L0.132[131,132]                                         |L0.132|                                    "
    - "L0.133[132,133]                                         |L0.133|                                    "
    - "L0.134[133,134]                                         |L0.134|                                    "
    - "L0.135[134,135]                                          |L0.135|                                   "
    - "L0.136[135,136]                                          |L0.136|                                   "
    - "L0.137[136,137]                                          |L0.137|                                   "
    - "L0.138[137,138]                                           |L0.138|                                  "
    - "L0.139[138,139]                                           |L0.139|                                  "
    - "L0.140[139,140]                                           |L0.140|                                  "
    - "L0.141[140,141]                                           |L0.141|                                  "
    - "L0.142[141,142]                                            |L0.142|                                 "
    - "L0.143[142,143]                                            |L0.143|                                 "
    - "L0.144[143,144]                                            |L0.144|                                 "
    - "L0.145[144,145]                                             |L0.145|                                "
    - "L0.146[145,146]                                             |L0.146|                                "
    - "L0.147[146,147]                                             |L0.147|                                "
    - "L0.148[147,148]                                             |L0.148|                                "
    - "L0.149[148,149]                                              |L0.149|                               "
    - "L0.150[149,150]                                              |L0.150|                               "
    - "L0.151[150,151]                                              |L0.151|                               "
    - "L0.152[151,152]                                              |L0.152|                               "
    - "L0.153[152,153]                                               |L0.153|                              "
    - "L0.154[153,154]                                               |L0.154|                              "
    - "L0.155[154,155]                                               |L0.155|                              "
    - "L0.156[155,156]                                                |L0.156|                             "
    - "L0.157[156,157]                                                |L0.157|                             "
    - "L0.158[157,158]                                                |L0.158|                             "
    - "L0.159[158,159]                                                |L0.159|                             "
    - "L0.160[159,160]                                                 |L0.160|                            "
    - "L0.161[160,161]                                                 |L0.161|                            "
    - "L0.162[161,162]                                                 |L0.162|                            "
    - "L0.163[162,163]                                                  |L0.163|                           "
    - "L0.164[163,164]                                                  |L0.164|                           "
    - "L0.165[164,165]                                                  |L0.165|                           "
    - "L0.166[165,166]                                                  |L0.166|                           "
    - "L0.167[166,167]                                                   |L0.167|                          "
    - "L0.168[167,168]                                                   |L0.168|                          "
    - "L0.169[168,169]                                                   |L0.169|                          "
    - "L0.170[169,170]                                                   |L0.170|                          "
    - "L0.171[170,171]                                                    |L0.171|                         "
    - "L0.172[171,172]                                                    |L0.172|                         "
    - "L0.173[172,173]                                                    |L0.173|                         "
    - "L0.174[173,174]                                                     |L0.174|                        "
    - "L0.175[174,175]                                                     |L0.175|                        "
    - "L0.176[175,176]                                                     |L0.176|                        "
    - "L0.177[176,177]                                                     |L0.177|                        "
    - "L0.178[177,178]                                                      |L0.178|                       "
    - "L0.179[178,179]                                                      |L0.179|                       "
    - "L0.180[179,180]                                                      |L0.180|                       "
    - "L0.181[180,181]                                                       |L0.181|                      "
    - "L0.182[181,182]                                                       |L0.182|                      "
    - "L0.183[182,183]                                                       |L0.183|                      "
    - "L0.184[183,184]                                                       |L0.184|                      "
    - "L0.185[184,185]                                                        |L0.185|                     "
    - "L0.186[185,186]                                                        |L0.186|                     "
    - "L0.187[186,187]                                                        |L0.187|                     "
    - "L0.188[187,188]                                                        |L0.188|                     "
    - "L0.189[188,189]                                                         |L0.189|                    "
    - "L0.190[189,190]                                                         |L0.190|                    "
    - "L0.191[190,191]                                                         |L0.191|                    "
    - "L0.192[191,192]                                                          |L0.192|                   "
    - "L0.193[192,193]                                                          |L0.193|                   "
    - "L0.194[193,194]                                                          |L0.194|                   "
    - "L0.195[194,195]                                                          |L0.195|                   "
    - "L0.196[195,196]                                                           |L0.196|                  "
    - "L0.197[196,197]                                                           |L0.197|                  "
    - "L0.198[197,198]                                                           |L0.198|                  "
    - "L0.199[198,199]                                                            |L0.199|                 "
    - "L0.200[199,200]                                                            |L0.200|                 "
    - "L0.201[200,201]                                                            |L0.201|                 "
    - "L0.202[201,202]                                                            |L0.202|                 "
    - "L0.203[202,203]                                                             |L0.203|                "
    - "L0.204[203,204]                                                             |L0.204|                "
    - "L0.205[204,205]                                                             |L0.205|                "
    - "L0.206[205,206]                                                             |L0.206|                "
    - "L0.207[206,207]                                                              |L0.207|               "
    - "L0.208[207,208]                                                              |L0.208|               "
    - "L0.209[208,209]                                                              |L0.209|               "
    - "L0.210[209,210]                                                               |L0.210|              "
    - "L0.211[210,211]                                                               |L0.211|              "
    - "L0.212[211,212]                                                               |L0.212|              "
    - "L0.213[212,213]                                                               |L0.213|              "
    - "L0.214[213,214]                                                                |L0.214|             "
    - "L0.215[214,215]                                                                |L0.215|             "
    - "L0.216[215,216]                                                                |L0.216|             "
    - "L0.217[216,217]                                                                 |L0.217|            "
    - "L0.218[217,218]                                                                 |L0.218|            "
    - "L0.219[218,219]                                                                 |L0.219|            "
    - "L0.220[219,220]                                                                 |L0.220|            "
    - "L0.221[220,221]                                                                  |L0.221|           "
    - "L0.222[221,222]                                                                  |L0.222|           "
    - "L0.223[222,223]                                                                  |L0.223|           "
    - "L0.224[223,224]                                                                  |L0.224|           "
    - "L0.225[224,225]                                                                   |L0.225|          "
    - "L0.226[225,226]                                                                   |L0.226|          "
    - "L0.227[226,227]                                                                   |L0.227|          "
    - "L0.228[227,228]                                                                    |L0.228|         "
    - "L0.229[228,229]                                                                    |L0.229|         "
    - "L0.230[229,230]                                                                    |L0.230|         "
    - "L0.231[230,231]                                                                    |L0.231|         "
    - "L0.232[231,232]                                                                     |L0.232|        "
    - "L0.233[232,233]                                                                     |L0.233|        "
    - "L0.234[233,234]                                                                     |L0.234|        "
    - "L0.235[234,235]                                                                      |L0.235|       "
    - "L0.236[235,236]                                                                      |L0.236|       "
    - "L0.237[236,237]                                                                      |L0.237|       "
    - "L0.238[237,238]                                                                      |L0.238|       "
    - "L0.239[238,239]                                                                       |L0.239|      "
    - "L0.240[239,240]                                                                       |L0.240|      "
    - "L0.241[240,241]                                                                       |L0.241|      "
    - "L0.242[241,242]                                                                       |L0.242|      "
    - "L0.243[242,243]                                                                        |L0.243|     "
    - "L0.244[243,244]                                                                        |L0.244|     "
    - "L0.245[244,245]                                                                        |L0.245|     "
    - "L0.246[245,246]                                                                         |L0.246|    "
    - "L0.247[246,247]                                                                         |L0.247|    "
    - "L0.248[247,248]                                                                         |L0.248|    "
    - "L0.249[248,249]                                                                         |L0.249|    "
    - "L0.250[249,250]                                                                          |L0.250|   "
    - "L0.251[250,251]                                                                          |L0.251|   "
    - "L0.252[251,252]                                                                          |L0.252|   "
    - "L0.253[252,253]                                                                           |L0.253|  "
    - "L0.254[253,254]                                                                           |L0.254|  "
    - "L0.255[254,255]                                                                           |L0.255|  "
    - "L0.256[255,256]                                                                           |L0.256|  "
    - "L0.257[256,257]                                                                            |L0.257| "
    - "L0.258[257,258]                                                                            |L0.258| "
    - "L0.259[258,259]                                                                            |L0.259| "
    - "L0.260[259,260]                                                                            |L0.260| "
    - "L0.261[260,261]                                                                             |L0.261|"
    - "L0.262[261,262]                                                                             |L0.262|"
    - "L0.263[262,263]                                                                             |L0.263|"
    - "L0.264[263,264]                                                                              |L0.264|"
    - "L0.265[264,265]                                                                              |L0.265|"
    - "L0.266[265,266]                                                                              |L0.266|"
    - "L0.267[266,267]                                                                              |L0.267|"
    - "L0.268[267,268]                                                                               |L0.268|"
    - "L0.269[268,269]                                                                               |L0.269|"
    - "L0.270[269,270]                                                                               |L0.270|"
    - "L0.271[270,271]                                                                                |L0.271|"
    - "L0.272[271,272]                                                                                |L0.272|"
    - "L0.273[272,273]                                                                                |L0.273|"
    - "L0.274[273,274]                                                                                |L0.274|"
    - "L0.275[274,275]                                                                                 |L0.275|"
    - "L0.276[275,276]                                                                                 |L0.276|"
    - "L0.277[276,277]                                                                                 |L0.277|"
    - "L0.278[277,278]                                                                                 |L0.278|"
    - "L0.279[278,279]                                                                                  |L0.279|"
    - "L0.280[279,280]                                                                                  |L0.280|"
    - "L0.281[280,281]                                                                                  |L0.281|"
    - "L0.282[281,282]                                                                                   |L0.282|"
    - "L0.283[282,283]                                                                                   |L0.283|"
    - "L0.284[283,284]                                                                                   |L0.284|"
    - "L0.285[284,285]                                                                                   |L0.285|"
    - "L0.286[285,286]                                                                                    |L0.286|"
    - "L0.287[286,287]                                                                                    |L0.287|"
    - "L0.288[287,288]                                                                                    |L0.288|"
    - "SKIPPED COMPACTION for PartitionId(1): partition 1 has 419430400 parquet file bytes, limit is 268435456"
    - "**** Final Output Files "
    - "L0, all files 2mb                                                                                   "
    - "L0.1[0,1]           |L0.1|                                                                          "
    - "L0.2[1,2]           |L0.2|                                                                          "
    - "L0.3[2,3]           |L0.3|                                                                          "
    - "L0.4[3,4]           |L0.4|                                                                          "
    - "L0.5[4,5]            |L0.5|                                                                         "
    - "L0.6[5,6]            |L0.6|                                                                         "
    - "L0.7[6,7]            |L0.7|                                                                         "
    - "L0.8[7,8]            |L0.8|                                                                         "
    - "L0.9[8,9]             |L0.9|                                                                        "
    - "L0.10[9,10]           |L0.10|                                                                       "
    - "L0.11[10,11]          |L0.11|                                                                       "
    - "L0.12[11,12]           |L0.12|                                                                      "
    - "L0.13[12,13]           |L0.13|                                                                      "
    - "L0.14[13,14]           |L0.14|                                                                      "
    - "L0.15[14,15]           |L0.15|                                                                      "
    - "L0.16[15,16]            |L0.16|                                                                     "
    - "L0.17[16,17]            |L0.17|                                                                     "
    - "L0.18[17,18]            |L0.18|                                                                     "
    - "L0.19[18,19]             |L0.19|                                                                    "
    - "L0.20[19,20]             |L0.20|                                                                    "
    - "L0.21[20,21]             |L0.21|                                                                    "
    - "L0.22[21,22]             |L0.22|                                                                    "
    - "L0.23[22,23]              |L0.23|                                                                   "
    - "L0.24[23,24]              |L0.24|                                                                   "
    - "L0.25[24,25]              |L0.25|                                                                   "
    - "L0.26[25,26]              |L0.26|                                                                   "
    - "L0.27[26,27]               |L0.27|                                                                  "
    - "L0.28[27,28]               |L0.28|                                                                  "
    - "L0.29[28,29]               |L0.29|                                                                  "
    - "L0.30[29,30]                |L0.30|                                                                 "
    - "L0.31[30,31]                |L0.31|                                                                 "
    - "L0.32[31,32]                |L0.32|                                                                 "
    - "L0.33[32,33]                |L0.33|                                                                 "
    - "L0.34[33,34]                 |L0.34|                                                                "
    - "L0.35[34,35]                 |L0.35|                                                                "
    - "L0.36[35,36]                 |L0.36|                                                                "
    - "L0.37[36,37]                  |L0.37|                                                               "
    - "L0.38[37,38]                  |L0.38|                                                               "
    - "L0.39[38,39]                  |L0.39|                                                               "
    - "L0.40[39,40]                  |L0.40|                                                               "
    - "L0.41[40,41]                   |L0.41|                                                              "
    - "L0.42[41,42]                   |L0.42|                                                              "
    - "L0.43[42,43]                   |L0.43|                                                              "
    - "L0.44[43,44]                   |L0.44|                                                              "
    - "L0.45[44,45]                    |L0.45|                                                             "
    - "L0.46[45,46]                    |L0.46|                                                             "
    - "L0.47[46,47]                    |L0.47|                                                             "
    - "L0.48[47,48]                     |L0.48|                                                            "
    - "L0.49[48,49]                     |L0.49|                                                            "
    - "L0.50[49,50]                     |L0.50|                                                            "
    - "L0.51[50,51]                     |L0.51|                                                            "
    - "L0.52[51,52]                      |L0.52|                                                           "
    - "L0.53[52,53]                      |L0.53|                                                           "
    - "L0.54[53,54]                      |L0.54|                                                           "
    - "L0.55[54,55]                       |L0.55|                                                          "
    - "L0.56[55,56]                       |L0.56|                                                          "
    - "L0.57[56,57]                       |L0.57|                                                          "
    - "L0.58[57,58]                       |L0.58|                                                          "
    - "L0.59[58,59]                        |L0.59|                                                         "
    - "L0.60[59,60]                        |L0.60|                                                         "
    - "L0.61[60,61]                        |L0.61|                                                         "
    - "L0.62[61,62]                        |L0.62|                                                         "
    - "L0.63[62,63]                         |L0.63|                                                        "
    - "L0.64[63,64]                         |L0.64|                                                        "
    - "L0.65[64,65]                         |L0.65|                                                        "
    - "L0.66[65,66]                          |L0.66|                                                       "
    - "L0.67[66,67]                          |L0.67|                                                       "
    - "L0.68[67,68]                          |L0.68|                                                       "
    - "L0.69[68,69]                          |L0.69|                                                       "
    - "L0.70[69,70]                           |L0.70|                                                      "
    - "L0.71[70,71]                           |L0.71|                                                      "
    - "L0.72[71,72]                           |L0.72|                                                      "
    - "L0.73[72,73]                            |L0.73|                                                     "
    - "L0.74[73,74]                            |L0.74|                                                     "
    - "L0.75[74,75]                            |L0.75|                                                     "
    - "L0.76[75,76]                            |L0.76|                                                     "
    - "L0.77[76,77]                             |L0.77|                                                    "
    - "L0.78[77,78]                             |L0.78|                                                    "
    - "L0.79[78,79]                             |L0.79|                                                    "
    - "L0.80[79,80]                             |L0.80|                                                    "
    - "L0.81[80,81]                              |L0.81|                                                   "
    - "L0.82[81,82]                              |L0.82|                                                   "
    - "L0.83[82,83]                              |L0.83|                                                   "
    - "L0.84[83,84]                               |L0.84|                                                  "
    - "L0.85[84,85]                               |L0.85|                                                  "
    - "L0.86[85,86]                               |L0.86|                                                  "
    - "L0.87[86,87]                               |L0.87|                                                  "
    - "L0.88[87,88]                                |L0.88|                                                 "
    - "L0.89[88,89]                                |L0.89|                                                 "
    - "L0.90[89,90]                                |L0.90|                                                 "
    - "L0.91[90,91]                                 |L0.91|                                                "
    - "L0.92[91,92]                                 |L0.92|                                                "
    - "L0.93[92,93]                                 |L0.93|                                                "
    - "L0.94[93,94]                                 |L0.94|                                                "
    - "L0.95[94,95]                                  |L0.95|                                               "
    - "L0.96[95,96]                                  |L0.96|                                               "
    - "L0.97[96,97]                                  |L0.97|                                               "
    - "L0.98[97,98]                                  |L0.98|                                               "
    - "L0.99[98,99]                                   |L0.99|                                              "
    - "L0.100[99,100]                                 |L0.100|                                             "
    - "L0.101[100,101]                                |L0.101|                                             "
    - "L0.102[101,102]                                 |L0.102|                                            "
    - "L0.103[102,103]                                 |L0.103|                                            "
    - "L0.104[103,104]                                 |L0.104|                                            "
    - "L0.105[104,105]                                 |L0.105|                                            "
    - "L0.106[105,106]                                  |L0.106|                                           "
    - "L0.107[106,107]                                  |L0.107|                                           "
    - "L0.108[107,108]                                  |L0.108|                                           "
    - "L0.109[108,109]                                   |L0.109|                                          "
    - "L0.110[109,110]                                   |L0.110|                                          "
    - "L0.111[110,111]                                   |L0.111|                                          "
    - "L0.112[111,112]                                   |L0.112|                                          "
    - "L0.113[112,113]                                    |L0.113|                                         "
    - "L0.114[113,114]                                    |L0.114|                                         "
    - "L0.115[114,115]                                    |L0.115|                                         "
    - "L0.116[115,116]                                    |L0.116|                                         "
    - "L0.117[116,117]                                     |L0.117|                                        "
    - "L0.118[117,118]                                     |L0.118|                                        "
    - "L0.119[118,119]                                     |L0.119|                                        "
    - "L0.120[119,120]                                      |L0.120|                                       "
    - "L0.121[120,121]                                      |L0.121|                                       "
    - "L0.122[121,122]                                      |L0.122|                                       "
    - "L0.123[122,123]                                      |L0.123|                                       "
    - "L0.124[123,124]                                       |L0.124|                                      "
    - "L0.125[124,125]                                       |L0.125|                                      "
    - "L0.126[125,126]                                       |L0.126|                                      "
    - "L0.127[126,127]                                        |L0.127|                                     "
    - "L0.128[127,128]                                        |L0.128|                                     "
    - "L0.129[128,129]                                        |L0.129|                                     "
    - "L0.130[129,130]                                        |L0.130|                                     "
    - "L0.131[130,131]                                         |L0.131|                                    "
    - "L0.132[131,132]                                         |L0.132|                                    "
    - "L0.133[132,133]                                         |L0.133|                                    "
    - "L0.134[133,134]                                         |L0.134|                                    "
    - "L0.135[134,135]                                          |L0.135|                                   "
    - "L0.136[135,136]                                          |L0.136|                                   "
    - "L0.137[136,137]                                          |L0.137|                                   "
    - "L0.138[137,138]                                           |L0.138|                                  "
    - "L0.139[138,139]                                           |L0.139|                                  "
    - "L0.140[139,140]                                           |L0.140|                                  "
    - "L0.141[140,141]                                           |L0.141|                                  "
    - "L0.142[141,142]                                            |L0.142|                                 "
    - "L0.143[142,143]                                            |L0.143|                                 "
    - "L0.144[143,144]                                            |L0.144|                                 "
    - "L0.145[144,145]                                             |L0.145|                                "
    - "L0.146[145,146]                                             |L0.146|                                "
    - "L0.147[146,147]                                             |L0.147|                                "
    - "L0.148[147,148]                                             |L0.148|                                "
    - "L0.149[148,149]                                              |L0.149|                               "
    - "L0.150[149,150]                                              |L0.150|                               "
    - "L0.151[150,151]                                              |L0.151|                               "
    - "L0.152[151,152]                                              |L0.152|                               "
    - "L0.153[152,153]                                               |L0.153|                              "
    - "L0.154[153,154]                                               |L0.154|                              "
    - "L0.155[154,155]                                               |L0.155|                              "
    - "L0.156[155,156]                                                |L0.156|                             "
    - "L0.157[156,157]                                                |L0.157|                             "
    - "L0.158[157,158]                                                |L0.158|                             "
    - "L0.159[158,159]                                                |L0.159|                             "
    - "L0.160[159,160]                                                 |L0.160|                            "
    - "L0.161[160,161]                                                 |L0.161|                            "
    - "L0.162[161,162]                                                 |L0.162|                            "
    - "L0.163[162,163]                                                  |L0.163|                           "
    - "L0.164[163,164]                                                  |L0.164|                           "
    - "L0.165[164,165]                                                  |L0.165|                           "
    - "L0.166[165,166]                                                  |L0.166|                           "
    - "L0.167[166,167]                                                   |L0.167|                          "
    - "L0.168[167,168]                                                   |L0.168|                          "
    - "L0.169[168,169]                                                   |L0.169|                          "
    - "L0.170[169,170]                                                   |L0.170|                          "
    - "L0.171[170,171]                                                    |L0.171|                         "
    - "L0.172[171,172]                                                    |L0.172|                         "
    - "L0.173[172,173]                                                    |L0.173|                         "
    - "L0.174[173,174]                                                     |L0.174|                        "
    - "L0.175[174,175]                                                     |L0.175|                        "
    - "L0.176[175,176]                                                     |L0.176|                        "
    - "L0.177[176,177]                                                     |L0.177|                        "
    - "L0.178[177,178]                                                      |L0.178|                       "
    - "L0.179[178,179]                                                      |L0.179|                       "
    - "L0.180[179,180]                                                      |L0.180|                       "
    - "L0.181[180,181]                                                       |L0.181|                      "
    - "L0.182[181,182]                                                       |L0.182|                      "
    - "L0.183[182,183]                                                       |L0.183|                      "
    - "L0.184[183,184]                                                       |L0.184|                      "
    - "L0.185[184,185]                                                        |L0.185|                     "
    - "L0.186[185,186]                                                        |L0.186|                     "
    - "L0.187[186,187]                                                        |L0.187|                     "
    - "L0.188[187,188]                                                        |L0.188|                     "
    - "L0.189[188,189]                                                         |L0.189|                    "
    - "L0.190[189,190]                                                         |L0.190|                    "
    - "L0.191[190,191]                                                         |L0.191|                    "
    - "L0.192[191,192]                                                          |L0.192|                   "
    - "L0.193[192,193]                                                          |L0.193|                   "
    - "L0.194[193,194]                                                          |L0.194|                   "
    - "L0.195[194,195]                                                          |L0.195|                   "
    - "L0.196[195,196]                                                           |L0.196|                  "
    - "L0.197[196,197]                                                           |L0.197|                  "
    - "L0.198[197,198]                                                           |L0.198|                  "
    - "L0.199[198,199]                                                            |L0.199|                 "
    - "L0.200[199,200]                                                            |L0.200|                 "
    - "L0.201[200,201]                                                            |L0.201|                 "
    - "L0.202[201,202]                                                            |L0.202|                 "
    - "L0.203[202,203]                                                             |L0.203|                "
    - "L0.204[203,204]                                                             |L0.204|                "
    - "L0.205[204,205]                                                             |L0.205|                "
    - "L0.206[205,206]                                                             |L0.206|                "
    - "L0.207[206,207]                                                              |L0.207|               "
    - "L0.208[207,208]                                                              |L0.208|               "
    - "L0.209[208,209]                                                              |L0.209|               "
    - "L0.210[209,210]                                                               |L0.210|              "
    - "L0.211[210,211]                                                               |L0.211|              "
    - "L0.212[211,212]                                                               |L0.212|              "
    - "L0.213[212,213]                                                               |L0.213|              "
    - "L0.214[213,214]                                                                |L0.214|             "
    - "L0.215[214,215]                                                                |L0.215|             "
    - "L0.216[215,216]                                                                |L0.216|             "
    - "L0.217[216,217]                                                                 |L0.217|            "
    - "L0.218[217,218]                                                                 |L0.218|            "
    - "L0.219[218,219]                                                                 |L0.219|            "
    - "L0.220[219,220]                                                                 |L0.220|            "
    - "L0.221[220,221]                                                                  |L0.221|           "
    - "L0.222[221,222]                                                                  |L0.222|           "
    - "L0.223[222,223]                                                                  |L0.223|           "
    - "L0.224[223,224]                                                                  |L0.224|           "
    - "L0.225[224,225]                                                                   |L0.225|          "
    - "L0.226[225,226]                                                                   |L0.226|          "
    - "L0.227[226,227]                                                                   |L0.227|          "
    - "L0.228[227,228]                                                                    |L0.228|         "
    - "L0.229[228,229]                                                                    |L0.229|         "
    - "L0.230[229,230]                                                                    |L0.230|         "
    - "L0.231[230,231]                                                                    |L0.231|         "
    - "L0.232[231,232]                                                                     |L0.232|        "
    - "L0.233[232,233]                                                                     |L0.233|        "
    - "L0.234[233,234]                                                                     |L0.234|        "
    - "L0.235[234,235]                                                                      |L0.235|       "
    - "L0.236[235,236]                                                                      |L0.236|       "
    - "L0.237[236,237]                                                                      |L0.237|       "
    - "L0.238[237,238]                                                                      |L0.238|       "
    - "L0.239[238,239]                                                                       |L0.239|      "
    - "L0.240[239,240]                                                                       |L0.240|      "
    - "L0.241[240,241]                                                                       |L0.241|      "
    - "L0.242[241,242]                                                                       |L0.242|      "
    - "L0.243[242,243]                                                                        |L0.243|     "
    - "L0.244[243,244]                                                                        |L0.244|     "
    - "L0.245[244,245]                                                                        |L0.245|     "
    - "L0.246[245,246]                                                                         |L0.246|    "
    - "L0.247[246,247]                                                                         |L0.247|    "
    - "L0.248[247,248]                                                                         |L0.248|    "
    - "L0.249[248,249]                                                                         |L0.249|    "
    - "L0.250[249,250]                                                                          |L0.250|   "
    - "L0.251[250,251]                                                                          |L0.251|   "
    - "L0.252[251,252]                                                                          |L0.252|   "
    - "L0.253[252,253]                                                                           |L0.253|  "
    - "L0.254[253,254]                                                                           |L0.254|  "
    - "L0.255[254,255]                                                                           |L0.255|  "
    - "L0.256[255,256]                                                                           |L0.256|  "
    - "L0.257[256,257]                                                                            |L0.257| "
    - "L0.258[257,258]                                                                            |L0.258| "
    - "L0.259[258,259]                                                                            |L0.259| "
    - "L0.260[259,260]                                                                            |L0.260| "
    - "L0.261[260,261]                                                                             |L0.261|"
    - "L0.262[261,262]                                                                             |L0.262|"
    - "L0.263[262,263]                                                                             |L0.263|"
    - "L0.264[263,264]                                                                              |L0.264|"
    - "L0.265[264,265]                                                                              |L0.265|"
    - "L0.266[265,266]                                                                              |L0.266|"
    - "L0.267[266,267]                                                                              |L0.267|"
    - "L0.268[267,268]                                                                               |L0.268|"
    - "L0.269[268,269]                                                                               |L0.269|"
    - "L0.270[269,270]                                                                               |L0.270|"
    - "L0.271[270,271]                                                                                |L0.271|"
    - "L0.272[271,272]                                                                                |L0.272|"
    - "L0.273[272,273]                                                                                |L0.273|"
    - "L0.274[273,274]                                                                                |L0.274|"
    - "L0.275[274,275]                                                                                 |L0.275|"
    - "L0.276[275,276]                                                                                 |L0.276|"
    - "L0.277[276,277]                                                                                 |L0.277|"
    - "L0.278[277,278]                                                                                 |L0.278|"
    - "L0.279[278,279]                                                                                  |L0.279|"
    - "L0.280[279,280]                                                                                  |L0.280|"
    - "L0.281[280,281]                                                                                  |L0.281|"
    - "L0.282[281,282]                                                                                   |L0.282|"
    - "L0.283[282,283]                                                                                   |L0.283|"
    - "L0.284[283,284]                                                                                   |L0.284|"
    - "L0.285[284,285]                                                                                   |L0.285|"
    - "L0.286[285,286]                                                                                    |L0.286|"
    - "L0.287[286,287]                                                                                    |L0.287|"
    - "L0.288[287,288]                                                                                    |L0.288|"
    "###
    );
}
