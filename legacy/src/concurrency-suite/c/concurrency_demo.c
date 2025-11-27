// c/concurrency_demo.c
#define _GNU_SOURCE
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <pthread.h>
#include <time.h>

typedef struct { int *buf, cap, head, tail; pthread_mutex_t mu; } Queue;
static void q_init(Queue *q, int cap){ q->buf=(int*)malloc(sizeof(int)*cap); q->cap=cap; q->head=0; q->tail=0; pthread_mutex_init(&q->mu,NULL); }
static void q_free(Queue *q){ free(q->buf); pthread_mutex_destroy(&q->mu); }
static void q_push(Queue *q, int v){ if(q->tail<q->cap) q->buf[q->tail++]=v; }
static int q_try_pop(Queue *q, int *out){ int ok=0; pthread_mutex_lock(&q->mu); if(q->head<q->tail){ *out=q->buf[q->head++]; ok=1; } pthread_mutex_unlock(&q->mu); return ok; }

typedef struct {
    Queue *q; pthread_barrier_t *bar;
    int *pulled,*uniques,*dupes,*misses;
    int *seen; pthread_mutex_t *acc_mu;
} WorkerArgs;

static void* worker_fn(void *arg){
    WorkerArgs *wa=(WorkerArgs*)arg; pthread_barrier_wait(wa->bar);
    int item; if(q_try_pop(wa->q,&item)){ pthread_mutex_lock(wa->acc_mu);
        (*wa->pulled)++; if(wa->seen[item]) (*wa->dupes)++; else { wa->seen[item]=1; (*wa->uniques)++; }
        pthread_mutex_unlock(wa->acc_mu);
    } else { pthread_mutex_lock(wa->acc_mu); (*wa->misses)++; pthread_mutex_unlock(wa->acc_mu); }
    return NULL;
}

typedef struct { int workers,pulled,unique,duplicates,misses; double spread_ms, success_rate; } Trial;
static double now_ms(){ struct timespec ts; clock_gettime(CLOCK_MONOTONIC,&ts); return ts.tv_sec*1000.0 + ts.tv_nsec/1e6; }

static Trial run_trial(int N){
    Queue q; q_init(&q,N); for(int i=0;i<N;i++) q_push(&q,i);
    int pulled=0,uniques=0,dupes=0,misses=0; int *seen=(int*)calloc(N,sizeof(int));
    pthread_mutex_t acc_mu; pthread_mutex_init(&acc_mu,NULL);
    pthread_barrier_t bar; pthread_barrier_init(&bar,NULL,N+1);
    WorkerArgs wa={ .q=&q,.bar=&bar,.pulled=&pulled,.uniques=&uniques,.dupes=&dupes,.misses=&misses,.seen=seen,.acc_mu=&acc_mu };
    pthread_t *ths=(pthread_t*)malloc(sizeof(pthread_t)*N); for(int i=0;i<N;i++) pthread_create(&ths[i],NULL,worker_fn,&wa);
    double t0=now_ms(); pthread_barrier_wait(&bar); for(int i=0;i<N;i++) pthread_join(ths[i],NULL); double t1=now_ms();
    Trial t={ .workers=N,.pulled=pulled,.unique=uniques,.duplicates=dupes,.misses=misses,.spread_ms=t1-t0,.success_rate= N? (double)pulled/N:1.0 };
    free(ths); pthread_barrier_destroy(&bar); pthread_mutex_destroy(&acc_mu); free(seen); q_free(&q); return t;
}

static double avg(const double *xs,int n){ if(n<=0) return 0.0; double s=0; for(int i=0;i<n;i++) s+=xs[i]; return s/n; }
static int cmp_d(const void*a,const void*b){ double x=*(const double*)a,y=*(const double*)b; return (x<y)?-1:(x>y); }
static double pctl(double *xs,int n,double p){ if(n<=0) return 0.0; qsort(xs,n,sizeof(double),cmp_d); int k=(int)(p*(n-1)+0.5); if(k<0)k=0; if(k>=n)k=n-1; return xs[k]; }

static char* strdup2(const char*s){ size_t n=strlen(s)+1; char* p=(char*)malloc(n); memcpy(p,s,n); return p; }
static int parse_workers(const char*s,int**out,int*outn){ char*tmp=strdup2(s); int cap=16,n=0; int*arr=(int*)malloc(sizeof(int)*cap); char*tok=strtok(tmp,",");
  while(tok){ int v=atoi(tok); if(v<=0){ free(arr); free(tmp); return -1; } if(n==cap){ cap*=2; arr=(int*)realloc(arr,sizeof(int)*cap); } arr[n++]=v; tok=strtok(NULL,","); }
  *out=arr; *outn=n; free(tmp); return 0; }

int main(int argc,char**argv){
    const char*workers_arg="10,20,40,80,160,320"; int reps=20; const char*csv_path="c_concurrency_results.csv";
    for(int i=1;i<argc;i++){ if(!strcmp(argv[i],"-workers")&&i+1<argc) workers_arg=argv[++i];
      else if(!strcmp(argv[i],"-reps")&&i+1<argc) reps=atoi(argv[++i]);
      else if(!strcmp(argv[i],"-csv")&&i+1<argc) csv_path=argv[++i];
      else if(!strcmp(argv[i],"-h")||!strcmp(argv[i],"--help")){ printf("Usage: %s [-workers 10,20,...] [-reps N] [-csv out.csv]\n",argv[0]); return 0; } }
    int *workers_list=NULL, wn=0; if(parse_workers(workers_arg,&workers_list,&wn)!=0||wn==0){ fprintf(stderr,"Bad -workers\n"); return 1; }

    printf("C   workers | avg_succ%% | p95_succ%% | p99_succ%% | avg_spread | p95_spread | p99_spread\n");
    FILE*csv=fopen(csv_path,"w"); if(!csv){ perror("open csv"); return 1; }
    fprintf(csv,"workers,avg_success_rate,p95_success_rate,p99_success_rate,avg_duplicates,p95_duplicates,p99_duplicates,avg_misses,p95_misses,p99_misses,avg_spread_ms,p95_spread_ms,p99_spread_ms\n");

    for(int wi=0;wi<wn;wi++){
        int W=workers_list[wi];
        double *succ=(double*)malloc(sizeof(double)*reps), *dups=(double*)malloc(sizeof(double)*reps),
               *miss=(double*)malloc(sizeof(double)*reps), *sprd=(double*)malloc(sizeof(double)*reps);
        for(int r=0;r<reps;r++){ Trial t=run_trial(W); succ[r]=t.success_rate; dups[r]=t.duplicates; miss[r]=t.misses; sprd[r]=t.spread_ms; }
        double avg_s=avg(succ,reps), p95_s=pctl(succ,reps,0.95), p99_s=pctl(succ,reps,0.99),
               avg_d=avg(dups,reps), p95_d=pctl(dups,reps,0.95), p99_d=pctl(dups,reps,0.99),
               avg_m=avg(miss,reps), p95_m=pctl(miss,reps,0.95), p99_m=pctl(miss,reps,0.99),
               avg_sp=avg(sprd,reps), p95_sp=pctl(sprd,reps,0.95), p99_sp=pctl(sprd,reps,0.99);

        printf("C  %7d | %9.2f | %10.2f | %10.2f | %10.3f | %10.3f | %10.3f\n",
               W, 100.0*avg_s, 100.0*p95_s, 100.0*p99_s, avg_sp, p95_sp, p99_sp);

        fprintf(csv,"%d,%.6f,%.6f,%.6f,%.6f,%.6f,%.6f,%.6f,%.6f,%.6f,%.3f,%.3f,%.3f\n",
                W, avg_s, p95_s, p99_s, avg_d, p95_d, p99_d, avg_m, p95_m, p99_m, avg_sp, p95_sp, p99_sp);

        free(succ); free(dups); free(miss); free(sprd);
    }
    fclose(csv); free(workers_list);
    printf("\nSaved CSV -> %s\n", csv_path); return 0;
}
