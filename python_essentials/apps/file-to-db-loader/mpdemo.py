import time 
import multiprocessing

def mySleep(n):
    print(f'sleep for {n}')
    time.sleep(n)


def main():
    l = [1,2,4,3,5,6,6,8,3]
    pool = multiprocessing.Pool(8)
    pool.map(mySleep,l)


if __name__ == '__main__':
    main()


