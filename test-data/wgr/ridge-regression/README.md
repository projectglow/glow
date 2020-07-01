####Test data explanation
The data provided for the ridge regression unit tests consist of ~100 variants across 3 chromosomes, ~100 individuals,
and 4 simulated phenotypes.  
* **blockedGT.snappy.parquet**:  Blocked genotype matrix representing 100 variants across 3 chromosomes.  Genotype 
data are represented in a sparse format, with size, indices, and values columns.  Missing genotype values have been 
mean imputed.
* **groupedIDs.snappy.parquet**:  File for mapping sample group IDs to lists of sample IDs.  Consists of a column 
containing group IDs and a column containing an array of sample IDs.  This dataset contains 10 groups, with ~10 
samples per group.
* **pts.csv**:  File containing simulated phenotypes.  Consists of a sample ID column and 4 phenotype columns.  Phenotypes
were simulated by randomly generating weights (b) for the (standardized) test genotypes (X) and mixing the resulting 
vector with a noise vector (e) with varying proportions (e.g.: phenotype_i = (Xb)*f_i + e*(1-f_i)).  Phenotype names 
follow a pattern of "simX" where X is an integer representing the explainable variance in the phenotype (e.g. sim100 
is 100% explainable with the underlying genotype data).
* **X0.csv**:  File containing the same genotype data contained in blockedGT.snappy.parquet in the flattened and 
standardized state (e.g., each row represents an individual, each column represents a standardized variant, where a 
standardized variant has been centered at 0 and standard scaled to have variance = 1).
* **X1.csv**:  File representing the output of 1 round of ridge reduction, in a flattened and standardized state.  
Each row represents an individual, each column represents the output of a ridge model prediction for a particular block 
of variants, a regularization (alpha) value, and a phenotype (e.g., the level 1 ridge model).
* **X2.csv**:  File representing the output of 2 rounds of ridge reduction, in a flattened and standardized state.
Each row represents an individual, each column represents the output of a level 2 ridge model prediction for a block of 
level 1 ridge predictions, a regularization (alpha) value, and a phenotype.  At this level, the block represents a 
chromosome.

The following files were generated to accelerate test runtime:

* **level1BlockedGt.snappy.parquet**

    indexdf = spark.read.parquet(f'{data_root}/groupedIDs.snappy.parquet')
    blockdf = spark.read.parquet(f'{data_root}/blockedGT.snappy.parquet').limit(5)

    group2ids = __get_sample_blocks(indexdf)
    stack0 = RidgeReducer(alphas)

    level1df = stack0.fit_transform(blockdf, labeldf, group2ids)
    level1df.coalesce(1).write.parquet(f'{data_root}/level1BlockedGt.snappy.parquet')

* **level2BlockedGt.snappy.parquet**

    indexdf = spark.read.parquet(f'{data_root}/groupedIDs.snappy.parquet')
    level1df = spark.read.parquet(f'{data_root}/level1BlockedGt.snappy.parquet')

    group2ids = __get_sample_blocks(indexdf)
    stack1 = RidgeReducer(alphas)

    level2df = stack1.fit_transform(level1df, labeldf, group2ids)
    level2df.coalesce(1).write.parquet(f'{data_root}/level2BlockedGt.snappy.parquet')

* **level1YHatLoco.csv**

    indexdf = spark.read.parquet(f'{data_root}/groupedIDs.snappy.parquet')
    level1df = spark.read.parquet(f'{data_root}/level1BlockedGT.snappy.parquet')

    group2ids = __get_sample_blocks(indexdf)
    regressor = RidgeRegression(alphas)
    model1df, cvdf = regressor.fit(level1df, labeldf, group2ids)

    all_y_hat_df = pd.DataFrame()
    for contig in ['chr_1', 'chr_2', 'chr_3']:
        loco_model_df = model1df.filter(~col('header').startswith(contig))
        loco_y_hat_df = regressor.transform(level1df, labeldf, group2ids, loco_model_df, cvdf)
        loco_y_hat_df['contigName'] = contig.split('_')[-1]
        all_y_hat_df = all_y_hat_df.append(loco_y_hat_df)

    y_hat_df = all_y_hat_df.set_index('contigName', append=True)
    y_hat_df.to_csv(f'{data_root}/level1YHatLoco.csv')

* **level2YHatLoco.csv**

    indexdf = spark.read.parquet(f'{data_root}/groupedIDs.snappy.parquet')
    level2df = spark.read.parquet(f'{data_root}/level2BlockedGT.snappy.parquet')

    group2ids = __get_sample_blocks(indexdf)
    regressor = RidgeRegression(alphas)
    model1df, cvdf = regressor.fit(level2df, labeldf, group2ids)

    all_y_hat_df = pd.DataFrame()
    for contig in ['chr_1', 'chr_2', 'chr_3']:
        loco_model_df = model1df.filter(~col('header').startswith(contig))
        loco_y_hat_df = regressor.transform(level2df, labeldf, group2ids, loco_model_df, cvdf)
        loco_y_hat_df['contigName'] = contig.split('_')[-1]
        all_y_hat_df = all_y_hat_df.append(loco_y_hat_df)

    y_hat_df = all_y_hat_df.set_index('contigName', append=True)
    y_hat_df.to_csv(f'{data_root}/level2YHatLoco.csv')
