use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};

use rand::Rng;
use std::convert::TryFrom;
use std::mem;

use delorean_table::Packer;

const BATCH_SIZES: [usize; 6] = [10, 100, 1000, 10_000, 100_000, 1_000_000];

fn packer_insert_non_null(c: &mut Criterion) {
    benchmark_push(c, "packer_insert_non_null", &BATCH_SIZES);
}

fn packer_insert_with_null(c: &mut Criterion) {
    benchmark_push_option(c, "packer_insert_with_null", &BATCH_SIZES);
}

fn packer_get_element(c: &mut Criterion) {
    benchmark_get(c, "packer_get_element", &BATCH_SIZES);
}

fn packer_iter(c: &mut Criterion) {
    benchmark_iter(c, "packer_iter", &BATCH_SIZES[..4]);
}

fn benchmark_push(c: &mut Criterion, benchmark_group_name: &str, batch_sizes: &[usize]) {
    let mut group = c.benchmark_group(benchmark_group_name);
    for &batch_size in batch_sizes {
        let input = vec![0_u64; batch_size];

        group.throughput(Throughput::Bytes(u64::try_from(input.len() * 8).unwrap()));

        group.bench_with_input(
            BenchmarkId::from_parameter(batch_size),
            &input,
            |b, input| {
                b.iter(|| {
                    let mut packer = Packer::with_capacity(input.len());
                    for v in input {
                        packer.push(*v);
                    }
                });
            },
        );
    }
    group.finish();
}

fn benchmark_push_option(c: &mut Criterion, benchmark_group_name: &str, batch_sizes: &[usize]) {
    let mut group = c.benchmark_group(benchmark_group_name);
    for &batch_size in batch_sizes {
        let mut rng = rand::thread_rng();
        let mut input = Vec::with_capacity(batch_size);
        // insert 10% null values
        for _ in 0..batch_size {
            if rng.gen_range(0, 10) == 0 {
                input.push(None);
            } else {
                input.push(Some(1_u64));
            }
        }
        group.throughput(Throughput::Bytes(
            u64::try_from(input.len() * mem::size_of::<Option<u64>>()).unwrap(),
        ));

        group.bench_with_input(
            BenchmarkId::from_parameter(batch_size),
            &input,
            |b, input| {
                b.iter(|| {
                    let mut packer = Packer::with_capacity(input.len());
                    for v in input {
                        packer.push_option(*v);
                    }
                });
            },
        );
    }
    group.finish();
}

fn benchmark_get(c: &mut Criterion, benchmark_group_name: &str, batch_sizes: &[usize]) {
    let mut group = c.benchmark_group(benchmark_group_name);
    for &batch_size in batch_sizes {
        let mut rng = rand::thread_rng();
        let mut input = Vec::with_capacity(batch_size);
        // insert 10% null values
        for _ in 0..batch_size {
            if rng.gen_range(0, 10) == 0 {
                input.push(None);
            } else {
                input.push(Some(1_u64));
            }
        }

        // ensure last value is non-null so that we exercise cold path.
        input[batch_size - 1] = Some(1_u64);

        group.throughput(Throughput::Bytes(
            u64::try_from(input.len() * mem::size_of::<Option<u64>>()).unwrap(),
        ));

        group.bench_with_input(
            BenchmarkId::from_parameter(batch_size),
            &input,
            |b, input| {
                let mut packer = Packer::with_capacity(input.len());
                for v in input {
                    packer.push_option(*v);
                }

                b.iter(|| {
                    // worst case getting last element.
                    packer.get(packer.num_rows() - 1);
                });
            },
        );
    }
    group.finish();
}

fn benchmark_iter(c: &mut Criterion, benchmark_group_name: &str, batch_sizes: &[usize]) {
    let mut group = c.benchmark_group(benchmark_group_name);
    for &batch_size in batch_sizes {
        let mut rng = rand::thread_rng();
        let mut input = Vec::with_capacity(batch_size);
        // insert 10% null values
        for _ in 0..batch_size {
            if rng.gen_range(0, 100) < 10 {
                input.push(None);
            } else {
                input.push(Some(1_u64));
            }
        }

        group.throughput(Throughput::Bytes(
            u64::try_from(input.len() * mem::size_of::<Option<u64>>()).unwrap(),
        ));

        group.bench_with_input(
            BenchmarkId::from_parameter(batch_size),
            &input,
            |b, input| {
                let mut packer = Packer::with_capacity(input.len());
                for v in input {
                    packer.push_option(*v);
                }

                let mut sum = 0;
                b.iter(|| {
                    for v in packer.iter() {
                        if let Some(v) = v {
                            sum += *v;
                        }
                    }
                });
            },
        );
    }
    group.finish();
}

criterion_group!(
    benches,
    packer_insert_non_null,
    packer_insert_with_null,
    packer_get_element,
    packer_iter
);
criterion_main!(benches);
