/*
 * This file is open source software, licensed to you under the terms
 * of the Apache License, Version 2.0 (the "License").  See the NOTICE file
 * distributed with this work for additional information regarding copyright
 * ownership.  You may not use this file except in compliance with the License.
 *
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
/*
 * Copyright (C) 2015 Cloudius Systems, Ltd.
 */

#pragma once

#ifndef SEASTAR_MODULE
#include <seastar/core/future.hh>
#include <seastar/core/chunked_fifo.hh>
#include <seastar/util/modules.hh>
#include <cassert>
#include <utility>
#endif

namespace seastar {

SEASTAR_MODULE_EXPORT_BEGIN

/// \addtogroup fiber-module
/// @{

/// \brief Shared/exclusive mutual exclusion.
///
/// Similar to \c std::shared_mutex, this class provides protection
/// for a shared resource, with two levels of access protection: shared
/// and exclusive.  Shared access allows multiple tasks to access the
/// shared resource concurrently, while exclusive access allows just
/// one task to access the resource at a time.
///
/// Note that many seastar tasks do not require protection at all,
/// since the seastar scheduler is not preemptive; however tasks that do
/// (by waiting on a future) may require explicit locking.
///
/// The \ref with_shared(shared_mutex&, Func&&) and
/// \ref with_lock(shared_mutex&, Func&&) provide exception-safe
/// wrappers for use with \c shared_mutex.
///
/// \see semaphore simpler mutual exclusion
class shared_mutex {
    unsigned _readers = 0;
    bool _writer = false;
    struct waiter {
        waiter(promise<>&& pr, bool for_write) : pr(std::move(pr)), for_write(for_write) {}
        promise<> pr;
        bool for_write;
    };
    chunked_fifo<waiter> _waiters;
public:
    shared_mutex() = default;
    shared_mutex(shared_mutex&&) = default;
    shared_mutex& operator=(shared_mutex&&) = default;
    shared_mutex(const shared_mutex&) = delete;
    void operator=(const shared_mutex&) = delete;
    /// Lock the \c shared_mutex for shared access
    ///
    /// \return a future that becomes ready when no exclusive access
    ///         is granted to anyone.
    future<> lock_shared() noexcept {
        if (try_lock_shared()) {
            return make_ready_future<>();
        }
        try {
            _waiters.emplace_back(promise<>(), false);
            return _waiters.back().pr.get_future();
        } catch (...) {
            return current_exception_as_future();
        }
    }
    /// Try to lock the \c shared_mutex for shared access
    ///
    /// \return true iff could acquire the lock for shared access.
    bool try_lock_shared() noexcept {
        if (!_writer && _waiters.empty()) {
            ++_readers;
            return true;
        }
        return false;
    }
    /// Unlocks a \c shared_mutex after a previous call to \ref lock_shared().
    void unlock_shared() noexcept {
        assert(_readers > 0);
        --_readers;
        wake();
    }
    /// Lock the \c shared_mutex for exclusive access
    ///
    /// \return a future that becomes ready when no access, shared or exclusive
    ///         is granted to anyone.
    future<> lock() noexcept {
        if (try_lock()) {
            return make_ready_future<>();
        }
        try {
            _waiters.emplace_back(promise<>(), true);
            return _waiters.back().pr.get_future();
        } catch (...) {
            return current_exception_as_future();
        }
    }
    /// Try to lock the \c shared_mutex for exclusive access
    ///
    /// \return true iff could acquire the lock for exclusive access.
    bool try_lock() noexcept {
        if (!_readers && !_writer) {
            _writer = true;
            return true;
        }
        return false;
    }
    /// Unlocks a \c shared_mutex after a previous call to \ref lock().
    void unlock() noexcept {
        assert(_writer);
        _writer = false;
        wake();
    }
private:
    void wake() noexcept {
        while (!_waiters.empty()) {
            auto& w = _waiters.front();
            // note: _writer == false in wake()
            if (w.for_write) {
                if (!_readers) {
                    _writer = true;
                    w.pr.set_value();
                    _waiters.pop_front();
                }
                break;
            } else { // for read
                ++_readers;
                w.pr.set_value();
                _waiters.pop_front();
            }
        }
    }
};

/// Executes a function while holding shared access to a resource.
///
/// Executes a function while holding shared access to a resource.  When
/// the function returns, the mutex is automatically unlocked.
///
/// \param sm a \ref shared_mutex guarding access to the shared resource
/// \param func callable object to invoke while the mutex is held for shared access
/// \return whatever \c func returns, as a future
///
/// \relates shared_mutex
template <typename Func>
SEASTAR_CONCEPT(
    requires (std::invocable<Func> && std::is_nothrow_move_constructible_v<Func>)
    inline
    futurize_t<std::invoke_result_t<Func>>
)
SEASTAR_NO_CONCEPT(
    inline
    std::enable_if_t<std::is_nothrow_move_constructible_v<Func>, futurize_t<std::result_of_t<Func ()>>>
)
with_shared(shared_mutex& sm, Func&& func) noexcept {
    return sm.lock_shared().then([&sm, func = std::forward<Func>(func)] () mutable {
        return futurize_invoke(func).finally([&sm] {
            sm.unlock_shared();
        });
    });
}

template <typename Func>
SEASTAR_CONCEPT(
    requires (std::invocable<Func> && !std::is_nothrow_move_constructible_v<Func>)
    inline
    futurize_t<std::invoke_result_t<Func>>
)
SEASTAR_NO_CONCEPT(
    inline
    std::enable_if_t<!std::is_nothrow_move_constructible_v<Func>, futurize_t<std::result_of_t<Func ()>>>
)
with_shared(shared_mutex& sm, Func&& func) noexcept {
    // FIXME: use a coroutine when c++17 support is dropped
    try {
        return do_with(std::forward<Func>(func), [&sm] (Func& func) {
            return sm.lock_shared().then([&func] {
                return func();
            }).finally([&sm] {
                sm.unlock_shared();
            });
        });
    } catch (...) {
        return futurize<std::invoke_result_t<Func>>::current_exception_as_future();
    }
}

/// Executes a function while holding exclusive access to a resource.
///
/// Executes a function while holding exclusive access to a resource.  When
/// the function returns, the mutex is automatically unlocked.
///
/// \param sm a \ref shared_mutex guarding access to the shared resource
/// \param func callable object to invoke while the mutex is held for shared access
/// \return whatever \c func returns, as a future
///
/// \relates shared_mutex
template <typename Func>
SEASTAR_CONCEPT(
    requires (std::invocable<Func> && std::is_nothrow_move_constructible_v<Func>)
    inline
    futurize_t<std::invoke_result_t<Func>>
)
SEASTAR_NO_CONCEPT(
    inline
    std::enable_if_t<std::is_nothrow_move_constructible_v<Func>, futurize_t<std::result_of_t<Func ()>>>
)
with_lock(shared_mutex& sm, Func&& func) noexcept {
    return sm.lock().then([&sm, func = std::forward<Func>(func)] () mutable {
        return futurize_invoke(func).finally([&sm] {
            sm.unlock();
        });
    });
}


template <typename Func>
SEASTAR_CONCEPT(
    requires (std::invocable<Func> && !std::is_nothrow_move_constructible_v<Func>)
    inline
    futurize_t<std::invoke_result_t<Func>>
)
SEASTAR_NO_CONCEPT(
    inline
    std::enable_if_t<!std::is_nothrow_move_constructible_v<Func>, futurize_t<std::result_of_t<Func ()>>>
)
with_lock(shared_mutex& sm, Func&& func) noexcept {
    // FIXME: use a coroutine when c++17 support is dropped
    try {
        return do_with(std::forward<Func>(func), [&sm] (Func& func) {
            return sm.lock().then([&func] {
                return func();
            }).finally([&sm] {
                sm.unlock();
            });
        });
    } catch (...) {
        return futurize<std::invoke_result_t<Func>>::current_exception_as_future();
    }
}


/// \brief RAII-based shared lock for a \ref shared_mutex object.
///
/// As soon as the destructor of an object of this class is called,
/// the corresponding \ref shared_mutex is unlocked.
///
/// The class is effectively a safe way to keep a \ref shared_mutex
/// locked and to unconditionally release it because no matter what
/// happens, the destructor should eventually be called.
///
/// The corresponding \ref shared_mutex object MUST be kept alive
/// throughout the lifetime of this object.
///
/// The class is NOT responsible for shared locking a \ref shared_mutex.
/// \see get_shared_lock(shared_mutex&)
/// \see unique_lock
class shared_lock {
private:
    shared_mutex* _mutex;

public:
    friend future<shared_lock> get_shared_lock(shared_mutex& mutex) noexcept;

private:
    shared_lock(shared_mutex& mutex) noexcept : _mutex{std::addressof(mutex)} {}
public:
    shared_lock(shared_lock&& other) noexcept
        : _mutex{std::exchange(other._mutex, nullptr)}
    {}
    shared_lock& operator=(shared_lock&& other) noexcept {
        if (this != std::addressof(other)) {
            this->~shared_lock();
            new (this) shared_lock{std::move(other)};
        }
        return *this;
    }

    shared_lock(const shared_lock&) = delete;
    shared_lock& operator=(const shared_lock&) = delete;

    ~shared_lock() noexcept {
        if (_mutex) {
            _mutex->unlock_shared();
        }
    }
};

/// \brief Construct a RAII-based shared lock corresponding to a given \ref shared_mutex object.
///
/// Since constructors cannot be exectued asynchronously, use this function
/// to shared lock a \ref shared_mutex and construct a corresponding
/// \ref unique_lock object that will unlock it later when it is being destructed.
///
/// The caller is responsible for keeping the corresponding \ref shared_mutex object
/// alive at least until the returned \ref shared_lock is destroyed.
inline future<shared_lock> get_shared_lock(shared_mutex& mutex) noexcept {
    return mutex.lock_shared().then([&mutex] {
        return make_ready_future<shared_lock>(shared_lock{mutex});
    });
}


/// \brief RAII-based lock for a \ref shared_mutex object.
///
/// As soon as the destructor of an object of this class is called,
/// the corresponding \ref shared_mutex is unlocked.
///
/// The class is effectively a safe way to keep a \ref shared_mutex
/// locked and to unconditionally release it because no matter what
/// happens, the destructor should eventually be called.
///
/// The corresponding \ref shared_mutex object MUST be kept alive
/// throughout the lifetime of this object.
///
/// The class is NOT responsible for locking a \ref shared_mutex.
/// \see get_unique_lock(shared_mutex&)
/// \see shared_lock
class unique_lock {
private:
    shared_mutex* _mutex;

public:
    friend future<unique_lock> get_unique_lock(shared_mutex& mutex) noexcept;

private:
    unique_lock(shared_mutex& mutex) noexcept : _mutex{std::addressof(mutex)} {}
public:
    unique_lock(unique_lock&& other) noexcept
        : _mutex{std::exchange(other._mutex, nullptr)}
    {}
    unique_lock& operator=(unique_lock&& other) noexcept {
        if (this != std::addressof(other)) {
            this->~unique_lock();
            new (this) unique_lock{std::move(other)};
        }
        return *this;
    }

    unique_lock(const unique_lock&) = delete;
    unique_lock& operator=(const unique_lock&) = delete;

    ~unique_lock() noexcept {
        if (_mutex) {
            _mutex->unlock();
        }
    }
};

/// \brief Construct a RAII-based lock corresponding to a given \ref shared_mutex object.
///
/// Since constructors cannot be exectued asynchronously, use this function
/// to lock a \ref shared_mutex and construct a corresponding \ref unique_lock object
/// that will unlock it later when it is being destructed.
///
/// The caller is responsible for keeping the corresponding \ref shared_mutex object
/// alive at least until the returned \ref unique_lock is destroyed.
inline future<unique_lock> get_unique_lock(shared_mutex& mutex) noexcept {
    return mutex.lock().then([&mutex] {
        return make_ready_future<unique_lock>(unique_lock{mutex});
    });
}

/// @}
SEASTAR_MODULE_EXPORT_END

}
