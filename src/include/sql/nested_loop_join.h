#pragma once
#include "sql/table.h"
#include "sql/table_vector_iterator.h"

namespace tpl::sql::test {
class NestedLoopJoinTest;
}  // namespace tpl::sql::test

namespace tpl::sql {


class NestedLoopJoin {
 public:
  // Function used to check if two projected rows match.
  using MatchFunction = bool (*)(VectorProjectionIterator *, VectorProjectionIterator *);

  /**
   * Main Constructor.
   * @param table1 is the outer table.
   * @param table2 is the inner table.
   * @param match_fn is the function that compare tuples.
   */
  NestedLoopJoin(const Table& table1, const Table& table2, MatchFunction match_fn)
      : table1_(table1)
      , table2_(table2)
      , match_fn_(match_fn){}

  // Iterator that returns one match at a time.
  class Iterator {
   public:
    /**
     * Constructor
     * @param parent is used to access the parent fields.
     */
    Iterator(NestedLoopJoin* parent)
        : tbi1_(parent->table1_)
        , tbi2_(parent->table2_)
        , parent_(parent)
        , ended_(false)
    {
      // Advance up the first match.
      // Take care of the special case in which there is no match.
      ended_ = ended_ || !tbi1_.Advance();
      ended_ = ended_ || !tbi2_.Advance();
      if (!ended_) {
        auto* vpi1 = tbi1_.vector_projection_iterator();
        auto* vpi2 = tbi2_.vector_projection_iterator();
        if (!parent_->match_fn_(vpi1, vpi2)) Advance();
      }
    }

    // Returns the current matching projected rows.
    /**
     * @return the current matching projected rows.
     */
    const std::pair<VectorProjectionIterator*, VectorProjectionIterator*> GetCurrMatch() {
      return {tbi1_.vector_projection_iterator(), tbi2_.vector_projection_iterator()};
    }

    /**
     * @return whether the iterator has reached its limits.
     */
    bool Ended() {
      return ended_;
    }

    /**
     * Advances the tables' iterators up to the next match.
     */
    void Advance() {
      auto* vpi1 = tbi1_.vector_projection_iterator();
      auto* vpi2 = tbi2_.vector_projection_iterator();
      do {
        bool advance = true;
        // First advance vpi2 up the end.
        if (vpi2->HasNext()) {
          vpi2->Advance();
          advance = !vpi2->HasNext();
        }
        // If vpi2 ended, advance tbi2.
        if (advance && tbi2_.Advance()) {
          vpi2->Reset();
          advance = false;
        }

        // If tbi2 also ended, advance vpi1 and reset tbi2.
        if (advance && vpi1->HasNext()) { // Then advance tbi1.
          vpi1->Advance();
          advance = !vpi1->HasNext();
          tbi2_.Reset(parent_->table2_);
        }

        // If vpi1 also ended, advance tbi1 and reset tbi2.
        if (advance && tbi1_.Advance()) {
          vpi1->Reset();
          // Reset tbi2 to the beginning.
          tbi2_.Reset(parent_->table2_);
          advance = false;
        }

        // If nothing can advance, the limits are reached.
        if (advance) {
          ended_ = true;
          break;
        }
      } while(!parent_->match_fn_(vpi1, vpi2));
    }
   private:
    TableVectorIterator tbi1_;
    TableVectorIterator tbi2_;
    NestedLoopJoin* parent_;
    bool ended_;
  };

  // Initialize an iterator
  Iterator Iterate() { return Iterator(this); }
 private:
  const Table& table1_;
  const Table& table2_;
  MatchFunction match_fn_;
};
}
