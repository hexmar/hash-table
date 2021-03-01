#include <iostream>
#include <random>
#include <mutex>
#include <shared_mutex>
#include <condition_variable>
#include <thread>
#include <sys/time.h>

template<class T>
class HashTable {
    class Item {
    public:
        unsigned int key;
        T *value;
        bool free;
        Item *old;
        std::mutex mutex;

        Item() : key(0), value(nullptr), free(true), old(nullptr) {}

        Item(unsigned int key, T *value) : key(key), value(value), free(false), old(nullptr) {}

        void Swap(Item &item) {
            Item *temp = new Item();
            temp->key = key;
            temp->value = value;
            temp->free = free;
            key = item.key;
            value = item.value;
            free = item.free;
            item.key = temp->key;
            item.value = temp->value;
            item.free = temp->free;
            temp->old = old;
            old = temp;
        }

        void SwapInternal(Item &item) {
            Item temp;
            temp.key = key;
            temp.value = value;
            temp.free = free;
            key = item.key;
            value = item.value;
            free = item.free;
            item.key = temp.key;
            item.value = temp.value;
            item.free = temp.free;
        }

        void RemoveOldValue(unsigned int key) {
            Item *iter = old;
            if (iter != nullptr) {
                if (iter->key == key) {
                    old = iter->old;
                    delete iter;
                    return;
                }
                while (iter->old != nullptr) {
                    if (iter->old->key == key) {
                        Item *del = iter->old;
                        iter->old = iter->old->old;
                        delete del;
                        return;
                    }
                    iter = iter->old;
                }
            }
        }
    };

    class HashFunc {
        unsigned int a, b;
        unsigned long long p, m;

    public:
        HashFunc(unsigned long long mod) : p(2147483659) {
            update(mod);
        }

        unsigned int operator()(unsigned int key) {
            return (((unsigned long long) a * (unsigned long long) key + (unsigned long long) b) % p) & (m - 1);
        };

        void update() {
            std::random_device randomDevice;
            std::mt19937 randomEngine(randomDevice());
            std::uniform_int_distribution<unsigned int> uniformDistributionA(1, p - 1);
            std::uniform_int_distribution<unsigned int> uniformDistributionB(0, p - 1);
            a = uniformDistributionA(randomEngine);
            b = uniformDistributionB(randomEngine);
        }

        void update(unsigned long long mod) {
            m = mod;
            update();
        }
    };

    class Table {
    public:
        HashFunc hashFunc;
        Item *table;
        unsigned int size;
        const /*TODO static*/ unsigned int re[28] = {17, 37, 67, 131, 257, 521, 1031, 2053, 4099, 8209, 16411, 32771,
                                                     65537, 131101, 262147, 524309, 1048583, 2097169, 4194319,
                                                     8388617, 16777259, 33554467, 67108879, 134217757, 268435459,
                                                     536870923, 1073741827, 2147483659};

        Table() : size(16), hashFunc(16) {}

        Item &operator[](unsigned int index) {
            if (index >= size)
                index = size - 1;
            return table[index];
        }

        unsigned int hash(unsigned int key) {
            return hashFunc(key);
        }

        bool rehash(HashTable *hashTable) {
            bool repeat;
            for (unsigned int i = 0; i < size; ++i) {
                if (!table[i].free) {
                    unsigned int hash = hashFunc(table[i].key);
                    if (hash != i) {
                        table[i].free = true;
                        repeat = hashTable->AddInternal(table[i].key, table[i].value);
                        if (repeat)
                            return true;
                    }
                }
                Item *holder = table[i].old;
                table[i].old = nullptr;
                while (holder != nullptr) {
                    Item *iter = holder;
                    holder = holder->old;
                    iter->old = nullptr;
                    hashTable->AddOldInternal(iter);
                }
            }
            return false;
        }

        void incSize() {
            size *= 2;
            hashFunc.update(size);
        }

        void UpdateHashFunc() {
            hashFunc.update();
        }

        void SetSize(unsigned int size_new) {
            size = size_new;
            hashFunc.update(size);
        }
    };

    Item *table;
    unsigned long long count, size;
    std::shared_timed_mutex rwmutex;
    std::mutex mutex;
    std::condition_variable_any cond_var;
    volatile bool rehashing;
    Table tables[2];

    void rehash(std::shared_lock<std::shared_timed_mutex> *shared_lock) {
        bool wait(true);
        if (!rehashing) {
            std::lock_guard<std::mutex> lock(mutex);
            if (!rehashing) {
                rehashing = true;
                wait = false;
            }

        }
        if (wait) {
            cond_var.wait(*shared_lock, [this] { return !rehashing; });
        } else {
            shared_lock->unlock();
            {
                std::unique_lock<std::shared_timed_mutex> lock(rwmutex);
                rehash_internal();
                rehashing = false;
                cond_var.notify_all();
            }
            shared_lock->lock();
        }
    }

    void rehash_internal() {
        bool repeat;
        if ((float) count / (float) size >= 0.5 && size < 4294967296) {
            auto sizeOld = size;
            auto tableOld = table;
            try {
                do {
                    table = new Item[size * 2];
                    size *= 2;
                    tables[0].table = table;
                    tables[1].table = table + size / 2;
                    tables[0].incSize();
                    tables[1].incSize();
                    unsigned int reps(0);
                    do {
                        repeat = false;
                        for (unsigned int i = 0; (i < sizeOld) && (!repeat); ++i) {
                            if (!tableOld[i].free)
                                repeat = AddInternal(tableOld[i].key, tableOld[i].value);
                            if (repeat) {
                                for (unsigned long long j = 0; j < size; ++j)
                                    table[j].free = true;
                                tables[0].UpdateHashFunc();
                                tables[1].UpdateHashFunc();
                            }
                        }
                    } while (repeat && (reps++ != 1024));
                    if (repeat)
                        delete table;
                } while (repeat);
                for (unsigned int i = 0; i < sizeOld; ++i)
                    while (tableOld[i].old != nullptr) {
                        Item *iter = tableOld[i].old;
                        tableOld[i].old = iter->old;
                        iter->old = nullptr;
                        AddOldInternal(iter);
                    }
                delete tableOld;
                return;
            } catch (std::bad_alloc) {
                size = sizeOld;
                table = tableOld;
                tables[0].SetSize(size / 2);
                tables[1].SetSize(size / 2);

            };
        }
        do {
            tables[0].UpdateHashFunc();
            tables[1].UpdateHashFunc();
            repeat = tables[0].rehash(this);
            if (!repeat)
                tables[1].rehash(this);
        } while (repeat);
    }

    bool AddInternal(unsigned int key, T *value) {
        Item insert(key, value);
        for (unsigned int j = 0, i = 0; j < 32; ++j, i = i ? 0 : 1) {
            tables[i][tables[i].hash(insert.key)].SwapInternal(insert);
            if (insert.free)
                return false;
        }
        for (unsigned long long i = 0; i < size; ++i)
            if (table[i].free) {
                table[i].SwapInternal(insert);
                break;
            }
        return true;
    }

    void AddOldInternal(Item *insert) {
        unsigned int hash = tables[0].hash(insert->key);
        insert->old = tables[0][hash].old;
        tables[0][hash].old = insert;
    }

public:
    class AddException {
    public:
        unsigned int key;
        T *value;

        AddException(unsigned int key, T *value) : key(key), value(value) {}
    };

    HashTable() : count(0), size(32), rehashing(false) {
        table = new Item[size];
        tables[0].table = table;
        tables[1].table = table + size / 2;
    }

    ~HashTable() {
    }

    bool Add(unsigned int key, T *value) {
        unsigned int reps(0), hash0, hash1;
        Item insert(key, value);
        std::shared_lock<std::shared_timed_mutex> table_lock(rwmutex);
        if (rehashing) {
            cond_var.wait(table_lock, [this] { return !rehashing; });
        }
        do {
            hash0 = tables[0].hash(insert.key);
            hash1 = tables[1].hash(insert.key);
            {
                std::lock_guard<std::mutex> lock0(tables[0][hash0].mutex);
                if (reps > 0)
                    tables[0][hash0].RemoveOldValue(insert.key);
                {
                    std::lock_guard<std::mutex> lock1(tables[1][hash1].mutex);
                    if (!tables[1][hash1].free && tables[1][hash1].key == insert.key) {
                        throw AddException(insert.key, insert.value);
                    }
                }
                if (!tables[0][hash0].free && tables[0][hash0].key == insert.key) {
                    throw AddException(insert.key, insert.value);
                }
                tables[0][hash0].Swap(insert);
                if (insert.free) {
                    tables[0][hash0].RemoveOldValue(insert.key);
                    {
                        std::lock_guard<std::mutex> lock_count(mutex);
                        ++count;
                    }
                    return true;
                }
            }
            for (unsigned int j = 0, i = 1; j < 31; ++j, i = i ? 0 : 1) {
                hash1 = hash0;
                hash0 = tables[i].hash(insert.key);
                std::unique_lock<std::mutex> lock(tables[i][hash0].mutex);
                tables[i][hash0].Swap(insert);
                lock.unlock();
                {
                    std::lock_guard<std::mutex> lock1(tables[i ? 0 : 1][hash1].mutex);
                    tables[i ? 0 : 1][hash1].RemoveOldValue(tables[i][hash0].key);
                }
                if (insert.free) {
                    lock.lock();
                    tables[i][hash0].RemoveOldValue(insert.key);
                    {
                        std::lock_guard<std::mutex> lock_count(mutex);
                        ++count;
                    }
                    return true;
                }
            }
            rehash(&table_lock);
        } while (reps++ != 0xffff);
        throw AddException(insert.key, insert.value);
    }

    T *Remove(unsigned int key) {
        unsigned int hash0, hash1;
        bool repeat;
        std::shared_lock<std::shared_timed_mutex> table_lock(rwmutex);
        do {
            repeat = false;
            if (rehashing) {
                cond_var.wait(table_lock, [this] { return !rehashing; });
            }
            hash0 = tables[0].hash(key);
            hash1 = tables[1].hash(key);
            {
                std::lock_guard<std::mutex> lock0(tables[0][hash0].mutex);
                {
                    std::lock_guard<std::mutex> lock1(tables[1][hash1].mutex);
                    if (!tables[0][hash0].free && tables[0][hash0].key == key) {
                        tables[0][hash0].free = true;
                        return tables[0][hash0].value;
                    }
                    if (!tables[1][hash1].free && tables[1][hash1].key == key) {
                        tables[1][hash1].free = true;
                        return tables[1][hash1].value;
                    }
                    Item *iter;
                    for (iter = tables[0][hash0].old; (!repeat) && (iter != nullptr); iter = iter->old) {
                        if (!iter->free && iter->key == key)
                            repeat = true;
                    }
                    for (iter = tables[1][hash1].old; (!repeat) && (iter != nullptr); iter = iter->old) {
                        if (!iter->free && iter->key == key)
                            repeat = true;
                    }
                }
            }
        } while (repeat);
        return nullptr;
    }

    T *GetValue(unsigned int key) {
        unsigned int hash0, hash1;
        bool repeat;
        std::shared_lock<std::shared_timed_mutex> table_lock(rwmutex);
        do {
            repeat = false;
            if (rehashing) {
                cond_var.wait(table_lock, [this] { return !rehashing; });
            }
            hash0 = tables[0].hash(key);
            hash1 = tables[1].hash(key);
            {
                std::lock_guard<std::mutex> lock0(tables[0][hash0].mutex);
                {
                    std::lock_guard<std::mutex> lock1(tables[1][hash1].mutex);
                    if (!tables[0][hash0].free && tables[0][hash0].key == key)
                        return tables[0][hash0].value;
                    if (!tables[1][hash1].free && tables[1][hash1].key == key)
                        return tables[1][hash1].value;
                    Item *iter;
                    for (iter = tables[0][hash0].old; (!repeat) && (iter != nullptr); iter = iter->old)
                        if (!iter->free && iter->key == key)
                            repeat = true;
                    for (iter = tables[1][hash1].old; (!repeat) && (iter != nullptr); iter = iter->old)
                        if (!iter->free && iter->key == key)
                            repeat = true;
                }
            }
        } while (repeat);
        return nullptr;
    }

    bool IsEmpty() {
        std::shared_lock<std::shared_timed_mutex> lock(rwmutex);
        return count == 0;
    }

    unsigned int GetCount() {
        std::shared_lock<std::shared_timed_mutex> lock(rwmutex);
        return count;
    }
};

void job(HashTable<int> *hash_table, unsigned int thread_number, int *p) {
    thread_number *= 0x8000;
    for (unsigned int i = 0; i < 0x8000; ++i) {
        try {
            hash_table->Add(i + thread_number, p);
        } catch (HashTable<int>::AddException) {}
    }
    if (thread_number == (31 * 0x8000))
        *p = 1;
    if (thread_number == 0) {
        for (unsigned int i = 0; i < 0x8000; ++i) {
            auto res = hash_table->GetValue(i);
            if (res == nullptr)
                std::cout << " ";
        }
    }
    if (thread_number == 0x8000) {
        for (unsigned int i = 0; i < 0x8000; ++i) {
            auto res = hash_table->Remove(i + thread_number);
            if (res == nullptr)
                std::cout << " ";
        }
    }
    if (thread_number == (2 * 0x8000)) {
        thread_number *= 20;
        for (unsigned int i = 0; i < 0x8000; ++i) {
            auto res = hash_table->GetValue(i + thread_number);
            if (res != nullptr)
                std::cout << " ";
        }
    }
}

int main() {
    timeval start_time, stop_time;
    volatile int a = 5;
    int *pt = (int *) &a;
    HashTable<int> hashTable;
    std::thread *threads[32];
    for (unsigned int i = 0; i < 32; ++i)
        threads[i] = new std::thread(job, &hashTable, i, pt);
    unsigned int c(0), c1(0);
    while (a == 5) {
        c = hashTable.GetCount();
        if (c != c1)
            std::cout << c << std::endl;
        c1 = c;
    }
    for (unsigned int i = 0; i < 32; ++i) {
        threads[i]->join();
        delete threads[i];
    }
    return 0;
}
