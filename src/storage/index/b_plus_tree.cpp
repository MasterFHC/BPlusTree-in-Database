#include "storage/index/b_plus_tree.h"

#include <iostream>
#include <sstream>
#include <string>

#include "buffer/lru_k_replacer.h"
#include "common/config.h"
#include "common/exception.h"
#include "common/logger.h"
#include "common/macros.h"
#include "common/rid.h"
#include "storage/index/index_iterator.h"
#include "storage/page/b_plus_tree_header_page.h"
#include "storage/page/b_plus_tree_internal_page.h"
#include "storage/page/b_plus_tree_leaf_page.h"
#include "storage/page/b_plus_tree_page.h"
#include "storage/page/page_guard.h"

namespace bustub
{

    INDEX_TEMPLATE_ARGUMENTS
        BPLUSTREE_TYPE::BPlusTree(std::string name, page_id_t header_page_id,
            BufferPoolManager* buffer_pool_manager,
            const KeyComparator& comparator, int leaf_max_size,
            int internal_max_size)
        : index_name_(std::move(name)),
        bpm_(buffer_pool_manager),
        comparator_(std::move(comparator)),
        leaf_max_size_(leaf_max_size),
        internal_max_size_(internal_max_size),
        header_page_id_(header_page_id)
    {
        WritePageGuard guard = bpm_->FetchPageWrite(header_page_id_);
        // In the original bpt, I fetch the header page
        // thus there's at least one page now
        auto root_header_page = guard.template AsMut<BPlusTreeHeaderPage>();
        // reinterprete the data of the page into "HeaderPage"
        root_header_page->root_page_id_ = INVALID_PAGE_ID;
        // set the root_id to INVALID
    }

    /*
     * Helper function to decide whether current b+tree is empty
     */
    INDEX_TEMPLATE_ARGUMENTS
        auto BPLUSTREE_TYPE::IsEmpty() const  ->  bool
    {
        ReadPageGuard guard = bpm_->FetchPageRead(header_page_id_);
        auto root_header_page = guard.template As<BPlusTreeHeaderPage>();
        bool is_empty = root_header_page->root_page_id_ == INVALID_PAGE_ID;
        // Just check if the root_page_id is INVALID
        // usage to fetch a page:
        // fetch the page guard   ->   call the "As" function of the page guard
        // to reinterprete the data of the page as "BPlusTreePage"
        return is_empty;
    }
    /*****************************************************************************
     * SEARCH
     *****************************************************************************/
     /*
      * Return the only value that associated with input key
      * This method is used for point query
      * @return : true means key exists
      */
    INDEX_TEMPLATE_ARGUMENTS
        auto BPLUSTREE_TYPE::GetValue(const KeyType& key,
            std::vector<ValueType>* result, Transaction* txn)
        -> bool
    {
        ReadPageGuard head_guard = bpm_->FetchPageRead(header_page_id_);
        if (head_guard.template As<BPlusTreeHeaderPage>()->root_page_id_ == INVALID_PAGE_ID) {
            return false;
        }
        else {
            //Crabbing Protocol
            Context context;
            context.read_set_.push_back(bpm_->FetchPageRead(head_guard.template As<BPlusTreeHeaderPage>()->root_page_id_));
            // ReadPageGuard guard = bpm_->FetchPageRead(head_guard.template As<BPlusTreeHeaderPage>()->root_page_id_);
            head_guard.Drop();
            auto now_page = context.read_set_.back().template As<BPlusTreePage>();
            while (!now_page->IsLeafPage()) {
                auto internal_page = reinterpret_cast<const InternalPage*>(now_page);
                int slot_num = BinaryFind(internal_page, key);
                if (slot_num == -1) {
                    return false;
                }
                context.read_set_.push_back(bpm_->FetchPageRead(internal_page->ValueAt(slot_num)));
                context.read_set_.pop_front();
                now_page = context.read_set_.back().template As<BPlusTreePage>();
            }
            auto leaf_page = reinterpret_cast<const LeafPage*>(now_page);
            int slot_num = BinaryFind(leaf_page, key);
            if (slot_num == -1) {
                return false;
            }
            result->push_back(leaf_page->ValueAt(slot_num));
            return true;
        }
    }

    /*****************************************************************************
     * INSERTION
     *****************************************************************************/
     /*
      * Insert constant key & value pair into b+ tree
      * if current tree is empty, start new tree, update root page id and insert
      * entry, otherwise insert into leaf page.
      * @return: since we only support unique key, if user try to insert duplicate
      * keys return false, otherwise return true.
      */
    INDEX_TEMPLATE_ARGUMENTS
        void BPLUSTREE_TYPE::SetRootPageId(page_id_t root_page_id)
    {
        WritePageGuard guard = bpm_->FetchPageWrite(header_page_id_);
        auto root_header_page = guard.template AsMut<BPlusTreeHeaderPage>();
        root_header_page->root_page_id_ = root_page_id;
        return;
    }

    INDEX_TEMPLATE_ARGUMENTS
        void BPLUSTREE_TYPE::InsertIntoLeaf(LeafPage* leaf_page, const KeyType& key,
            const ValueType& value, Transaction* txn)
    {
        leaf_page->IncreaseSize(1);
        int slot_cnt = leaf_page->GetSize() - 1;
        for (int i = slot_cnt; i > 0; i--) {
            if (comparator_(leaf_page->KeyAt(i - 1), key) == 1) {
                leaf_page->SetAt(i, leaf_page->KeyAt(i - 1), leaf_page->ValueAt(i - 1));
            }
            else {
                leaf_page->SetAt(i, key, value);
                return;
            }
        }
        leaf_page->SetAt(0, key, value);
        return;
    }

    INDEX_TEMPLATE_ARGUMENTS
        void BPLUSTREE_TYPE::InsertIntoInternal(InternalPage* internal_page, const KeyType& key,
            const page_id_t& value, Transaction* txn)
    {
        internal_page->IncreaseSize(1);
        int slot_cnt = internal_page->GetSize() - 1;
        for (int i = slot_cnt; i > 1; i--) {
            if (comparator_(internal_page->KeyAt(i - 1), key) == 1) {
                internal_page->SetKeyAt(i, internal_page->KeyAt(i - 1));
                internal_page->SetValueAt(i, internal_page->ValueAt(i - 1));
            }
            else {
                internal_page->SetKeyAt(i, key);
                internal_page->SetValueAt(i, value);
                return;
            }
        }
        internal_page->SetKeyAt(1, key);
        internal_page->SetValueAt(1, value);
        return;
    }

    INDEX_TEMPLATE_ARGUMENTS
        auto BPLUSTREE_TYPE::Insert(const KeyType& key, const ValueType& value,
            Transaction* txn)  ->  bool
    {
        //Create a Context class to keep track of the pages on the path to the leaf page
        Context context;
        context.header_page_ = bpm_->FetchPageWrite(header_page_id_);
        if (context.header_page_.value().template As<BPlusTreeHeaderPage>()->root_page_id_ == INVALID_PAGE_ID) {
            // Create a B+ tree
            page_id_t new_id;
            auto root_guard = bpm_->NewPageGuarded(&new_id);

            //SetRootPageId(new_id);
            auto root_header_page = context.header_page_.value().template AsMut<BPlusTreeHeaderPage>();
            root_header_page->root_page_id_ = new_id;

            context.header_page_.value().Drop();
            auto now_page = root_guard.template AsMut<LeafPage>();
            now_page->Init();
            now_page->IncreaseSize(1);

            //leaf page starts at index 0
            now_page->SetAt(0, key, value);
            root_guard.Drop();
            return true;
        }
        else {
            // Insert into leaf page
            context.write_set_.push_back(bpm_->FetchPageWrite(context.header_page_.value().template AsMut<BPlusTreeHeaderPage>()->root_page_id_));
            page_id_t now_page_id = context.write_set_.back().PageId();
            auto now_page = context.write_set_.back().template AsMut<BPlusTreePage>();
            while (!now_page->IsLeafPage()) {
                auto internal_page = reinterpret_cast<InternalPage*>(now_page);
                int slot_num = BinaryFind(internal_page, key);
                if (slot_num == -1) {
                    return false;
                }
                context.write_set_.push_back(bpm_->FetchPageWrite(internal_page->ValueAt(slot_num)));
                now_page = context.write_set_.back().template AsMut<BPlusTreePage>();
                now_page_id = context.write_set_.back().PageId();
            }

            while (!context.write_set_.empty()) {
                //now repeat until the root page is reached 
                // or no more split is needed (containing the case where a new root is created)
                if (now_page->IsLeafPage()) {
                    auto leaf_page = reinterpret_cast<LeafPage*>(now_page);
                    int slot_num = BinaryFind(leaf_page, key);
                    if (slot_num != -1) {
                        return false;
                    }
                    InsertIntoLeaf(leaf_page, key, value, txn);

                    // std::cout<<DrawBPlusTree()<<std::endl;

                    //now detect whether a split is needed
                    if (leaf_page->GetSize() <= leaf_max_size_) {
                        while(!context.write_set_.empty()){
                            context.write_set_.pop_front();
                        }
                        return true;
                    }
                    //split
                    page_id_t new_page_id;
                    auto new_page_guard = bpm_->NewPageGuarded(&new_page_id);
                    auto new_page = new_page_guard.template AsMut<LeafPage>();
                    new_page->Init();
                    auto split_point = leaf_page->GetSize() / 2;
                    for (auto i = split_point; i < leaf_page->GetSize(); i++) {
                        new_page->IncreaseSize(1);
                        new_page->SetAt(i - split_point, leaf_page->KeyAt(i), leaf_page->ValueAt(i));
                    }
                    new_page->SetNextPageId(leaf_page->GetNextPageId());
                    leaf_page->SetNextPageId(new_page_id);
                    if(new_page->GetNextPageId() == INVALID_PAGE_ID){
                        //if the new page is the last page
                        new_page->SetLastPageId(now_page_id);
                    }
                    else{
                        //if the new page is not the last page
                        auto next_page_guard = bpm_->FetchPageWrite(new_page->GetNextPageId());
                        auto next_page = next_page_guard.template AsMut<LeafPage>();
                        new_page->SetLastPageId(now_page_id);
                        next_page->SetLastPageId(new_page_id);
                    }
                    leaf_page->SetSize(split_point);

                    //debug begin
                    // for(auto i=0;i<leaf_page->GetSize();i++){
                    //     std::cout<<"leafkey="<<leaf_page->KeyAt(i).ToString()<<", leafvalue="<<leaf_page->ValueAt(i)<<std::endl;
                    // }
                    // for(auto i=0;i<new_page->GetSize();i++){
                    //     std::cout<<"newkey="<<new_page->KeyAt(i).ToString()<<", newvalue="<<new_page->ValueAt(i)<<std::endl;
                    // }
                    //debug end


                    //if this node has no father, simply return after creating a new father
                    const page_id_t left_son_id = now_page_id, right_son_id = new_page_id;
                    if (context.write_set_.size() == 1) {
                        //create a new father
                        page_id_t fa_page_id;
                        auto fa_page_guard = bpm_->NewPageGuarded(&fa_page_id);
                        auto fa_page = fa_page_guard.template AsMut<InternalPage>();
                        fa_page->Init();
                        fa_page->IncreaseSize(1);
                        fa_page->SetValueAt(0, left_son_id);
                        fa_page->SetValueAt(1, right_son_id);
                        fa_page->SetKeyAt(1, new_page->KeyAt(0));

                        // SetRootPageId(fa_page_id);
                        auto root_header_page = context.header_page_.value().template AsMut<BPlusTreeHeaderPage>();
                        root_header_page->root_page_id_ = fa_page_id;

                        return true;
                    }
                    //if this node has a father
                    context.write_set_.pop_back();
                    auto fa_page = context.write_set_.back().template AsMut<InternalPage>();
                    InsertIntoInternal(fa_page, new_page->KeyAt(0), right_son_id, txn);

                    //refresh now_page and now_page_id
                    now_page = fa_page;
                    now_page_id = context.write_set_.back().PageId();
                }
                else {
                    //if this page is a internal page
                    auto internal_page = reinterpret_cast<InternalPage*>(now_page);
                    if (internal_page->GetSize() <= internal_max_size_) {
                        break;
                    }
                    //else, split
                    page_id_t new_page_id;
                    auto new_page_guard = bpm_->NewPageGuarded(&new_page_id);
                    auto new_page = new_page_guard.template AsMut<InternalPage>();
                    new_page->Init();
                    auto split_point = internal_page->GetSize() / 2;
                    auto split_key = internal_page->KeyAt(split_point);
                    new_page->SetValueAt(0, internal_page->ValueAt(split_point));
                    for (auto i = split_point + 1; i < internal_page->GetSize(); i++) {
                        new_page->IncreaseSize(1);
                        new_page->SetKeyAt(i - split_point, internal_page->KeyAt(i));
                        new_page->SetValueAt(i - split_point, internal_page->ValueAt(i));
                    }
                    internal_page->SetSize(split_point);

                    //if this node has no father, create a new father and change root_page_id
                    const page_id_t left_son_id = now_page_id, right_son_id = new_page_id;
                    if (context.write_set_.size() == 1) {
                        //create a new father
                        page_id_t fa_page_id;
                        auto fa_page_guard = bpm_->NewPageGuarded(&fa_page_id);
                        auto fa_page = fa_page_guard.template AsMut<InternalPage>();
                        fa_page->Init();
                        fa_page->IncreaseSize(1);
                        fa_page->SetValueAt(0, left_son_id);
                        fa_page->SetValueAt(1, right_son_id);
                        fa_page->SetKeyAt(1, split_key);

                        // SetRootPageId(fa_page_id);
                        auto root_header_page = context.header_page_.value().template AsMut<BPlusTreeHeaderPage>();
                        root_header_page->root_page_id_ = fa_page_id;

                        return true;
                    }
                    //if this leaf has a father
                    context.write_set_.pop_back();
                    auto fa_page = context.write_set_.back().template AsMut<InternalPage>();
                    InsertIntoInternal(fa_page, split_key, right_son_id, txn);

                    //refresh now_page and now_page_id
                    now_page = fa_page;
                    now_page_id = context.write_set_.back().PageId();
                }
            }

        }
        return true;
    }


    /*****************************************************************************
     * REMOVE
     *****************************************************************************/
     /*
      * Delete key & value pair associated with input key
      * If current tree is empty, return immediately.
      * If not, User needs to first find the right leaf page as deletion target, then
      * delete entry from leaf page. Remember to deal with redistribute or merge if
      * necessary.
      */
    INDEX_TEMPLATE_ARGUMENTS
        void BPLUSTREE_TYPE::RemoveFromLeaf(LeafPage* leaf_page, const KeyType& key, Transaction* txn)
    {
        int slot_cnt = leaf_page->GetSize() - 1;
        int slot_num = BinaryFind(leaf_page, key);
        for (int i = slot_num; i < slot_cnt; i++) {
            leaf_page->SetAt(i, leaf_page->KeyAt(i + 1), leaf_page->ValueAt(i + 1));
        }
        leaf_page->IncreaseSize(-1);
        return;
    }

    INDEX_TEMPLATE_ARGUMENTS
        void BPLUSTREE_TYPE::RemoveFromInternalWithIndex(InternalPage* internal_page, const int& index, Transaction* txn)
    {
        int slot_cnt = internal_page->GetSize() - 1;
        for (int i = index; i < slot_cnt; i++) {
            internal_page->SetKeyAt(i, internal_page->KeyAt(i + 1));
            internal_page->SetValueAt(i, internal_page->ValueAt(i + 1));
        }
        internal_page->IncreaseSize(-1);
        return;
    }

    
    INDEX_TEMPLATE_ARGUMENTS
        void BPLUSTREE_TYPE::Remove(const KeyType& key, Transaction* txn)
    {
        BasicPageGuard head_guard = bpm_->FetchPageBasic(header_page_id_);
        if (head_guard.template As<BPlusTreeHeaderPage>()->root_page_id_ == INVALID_PAGE_ID) {
            //if the tree is empty, simply return
            return;
        }
        Context context;
        context.header_page_ = bpm_->FetchPageWrite(header_page_id_);
        context.basic_set_.push_back(bpm_->FetchPageBasic(head_guard.template As<BPlusTreeHeaderPage>()->root_page_id_));
        page_id_t now_page_id = context.basic_set_.back().PageId();
        auto now_page = context.basic_set_.back().template AsMut<BPlusTreePage>();
        head_guard.Drop();
        while (!now_page->IsLeafPage()) {
            auto internal_page = reinterpret_cast<InternalPage*>(now_page);
            int slot_num = BinaryFind(internal_page, key);
            if (slot_num == -1) {
                return;
            }
            context.basic_set_.push_back(bpm_->FetchPageBasic(internal_page->ValueAt(slot_num)));
            now_page = context.basic_set_.back().template AsMut<BPlusTreePage>();
            now_page_id = context.basic_set_.back().PageId();
        }

        while (!context.basic_set_.empty()) {
            //now repeat until the root page is reached
            // or no more merge is needed 
            if (now_page->IsLeafPage()) {
                auto leaf_page = reinterpret_cast<LeafPage*>(now_page);
                int slot_num = BinaryFind(leaf_page, key);
                if (slot_num == -1) {
                    return;
                }
                RemoveFromLeaf(leaf_page, key, txn);

                if (now_page->GetSize() >= ((leaf_max_size_/2 > 0)?(leaf_max_size_/2):1)) {
                    //recursively check if father page needs to be updated
                    auto now_internal_page = reinterpret_cast<InternalPage*>(now_page);
                    bool is_leaf = now_page->IsLeafPage();
                    while(context.basic_set_.size() > 1){
                        auto fa_page = context.basic_set_[context.basic_set_.size()-2].template AsMut<InternalPage>();
                        auto fa_slot_num = BinaryFind(fa_page, key);
                        if(fa_slot_num == 0){
                            break;
                        }
                        else if(fa_slot_num == 1){ 
                            if(is_leaf){
                                fa_page->SetKeyAt(1, leaf_page->KeyAt(0));
                                is_leaf = false;
                            }
                            else{
                                fa_page->SetKeyAt(1, now_internal_page->KeyAt(1));
                            }
                            now_internal_page = fa_page;
                            context.basic_set_.pop_back();
                        }
                        else{
                            if(is_leaf){
                                fa_page->SetKeyAt(fa_slot_num, leaf_page->KeyAt(0));
                                is_leaf = false;
                            }
                            else{
                                fa_page->SetKeyAt(fa_slot_num, now_internal_page->KeyAt(1));
                            }
                            break;
                        }
                    }
                    break;
                }
                //merge
                if (context.basic_set_.size() == 1) {
                    //if this is the root page
                    if (now_page->GetSize() == 0) {
                        //if the root page is empty, delete it
                        // SetRootPageId(INVALID_PAGE_ID);
                        auto root_header_page = context.header_page_.value().template AsMut<BPlusTreeHeaderPage>();
                        root_header_page->root_page_id_ = INVALID_PAGE_ID;
                        return;
                    }
                    break;
                }
                //if this is not the root page
                // fa_guard = context.basic_set_[context.basic_set_.size()-2];
                // find left_brother(if exist) and try to redistribute
                auto fa_page = context.basic_set_[context.basic_set_.size() - 2].template AsMut<InternalPage>();
                int fa_slot_num = BinaryFind(fa_page, key);
                page_id_t bro_page_id;
                if(fa_slot_num == 0){
                    //if the key is the first key in the leaf page, try right_brother
                    bro_page_id = fa_page->ValueAt(1);
                }
                else{
                    bro_page_id = fa_page->ValueAt(fa_slot_num - 1);
                }
                auto bro_guard = bpm_->FetchPageBasic(bro_page_id);
                auto bro_page = bro_guard.template AsMut<LeafPage>();
                if (bro_page->GetSize() > ((leaf_max_size_/2 > 0)?(leaf_max_size_/2):1)) {
                    //redistribute
                    if(fa_slot_num == 0){
                        //if the key is the first key in father page, update the key in the father page
                        //and steal from the right brother
                        KeyType new_key = bro_page->KeyAt(0);
                        ValueType new_value = bro_page->ValueAt(0);
                        RemoveFromLeaf(bro_page, new_key, txn);
                        InsertIntoLeaf(leaf_page, new_key, new_value, txn);
                        fa_page->SetKeyAt(1, bro_page->KeyAt(0));
                    }
                    else{
                        //if the key is not the first key in father page, steal from the left brother
                        KeyType new_key = bro_page->KeyAt(bro_page->GetSize()-1);
                        ValueType new_value = bro_page->ValueAt(bro_page->GetSize()-1);
                        RemoveFromLeaf(bro_page, new_key, txn);
                        InsertIntoLeaf(leaf_page, new_key, new_value, txn);
                        fa_page->SetKeyAt(fa_slot_num, new_key);
                    }
                    //recursively check if father page needs to be updated
                    auto now_internal_page = reinterpret_cast<InternalPage*>(now_page);
                    while(context.basic_set_.size() > 1){
                        bool is_leaf = now_page->IsLeafPage();
                        auto fa_page = context.basic_set_[context.basic_set_.size()-2].template AsMut<InternalPage>();
                        auto fa_slot_num = BinaryFind(fa_page, key);
                        if(fa_slot_num == 0){
                            break;
                        }
                        else if(fa_slot_num == 1){ 
                            if(is_leaf){
                                fa_page->SetKeyAt(1, leaf_page->KeyAt(0));
                                is_leaf = false;
                            }
                            else{
                                fa_page->SetKeyAt(1, now_internal_page->KeyAt(1));
                            }
                            now_internal_page = fa_page;
                            context.basic_set_.pop_back();
                        }
                        else{
                            if(is_leaf){
                                fa_page->SetKeyAt(fa_slot_num, leaf_page->KeyAt(0));
                                is_leaf = false;
                            }
                            else{
                                fa_page->SetKeyAt(fa_slot_num, now_internal_page->KeyAt(1));
                            }
                            break;
                        }
                    }
                }
                else{
                    //merge, fa page will change, therefore we need to iterate the loop
                    if(fa_slot_num == 0){
                        //if the key is the first key in father page, merge with the right brother
                        for(auto i=0;i<leaf_page->GetSize();i++){
                            // bro_page->IncreaseSize(1);
                            InsertIntoLeaf(bro_page, leaf_page->KeyAt(i), leaf_page->ValueAt(i), txn);
                        }
                        auto last_page_id = leaf_page->GetLastPageId();
                        if(last_page_id != INVALID_PAGE_ID){
                            auto last_page_guard = bpm_->FetchPageBasic(last_page_id);
                            auto last_page = last_page_guard.template AsMut<LeafPage>();
                            last_page->SetNextPageId(bro_page->GetNextPageId());
                        }
                        bro_page->SetLastPageId(last_page_id);
                        fa_page->SetKeyAt(0, fa_page->KeyAt(1));
                        // RemoveFromLeaf(bro_page, key, txn);
                        if(fa_page ->GetSize() == 2){
                            //if the father page has only one key, delete it
                            // SetRootPageId(bro_page_id);
                            auto root_header_page = context.header_page_.value().template AsMut<BPlusTreeHeaderPage>();
                            root_header_page->root_page_id_ = bro_page_id;
                            RemoveFromInternalWithIndex(fa_page, 1, txn);
                            bpm_->DeletePage(now_page_id);
                            bpm_->DeletePage(context.basic_set_[context.basic_set_.size()-2].PageId());
                            return;
                        }
                        RemoveFromInternalWithIndex(fa_page, 1, txn);
                        //delete leaf_page
                        bpm_->DeletePage(now_page_id);
                    }
                    else{
                        //if the key is not the first key in father page, merge with the left brother
                        for(auto i=0;i<leaf_page->GetSize();i++){
                            // bro_page->IncreaseSize(1);
                            InsertIntoLeaf(bro_page, leaf_page->KeyAt(i), leaf_page->ValueAt(i), txn);
                        }
                        auto next_page_id = leaf_page->GetNextPageId();
                        if(next_page_id != INVALID_PAGE_ID){
                            auto next_page_guard = bpm_->FetchPageBasic(next_page_id);
                            auto next_page = next_page_guard.template AsMut<LeafPage>();
                            next_page->SetLastPageId(bro_page_id);
                        }
                        bro_page->SetNextPageId(leaf_page->GetNextPageId());
                        // RemoveFromLeaf(bro_page, key, txn);
                        if(fa_page ->GetSize() == 2){
                            //if the father page has only one key, delete it
                            // SetRootPageId(bro_page_id);
                            auto root_header_page = context.header_page_.value().template AsMut<BPlusTreeHeaderPage>();
                            root_header_page->root_page_id_ = bro_page_id;
                            RemoveFromInternalWithIndex(fa_page, fa_slot_num, txn);
                            bpm_->DeletePage(now_page_id);
                            bpm_->DeletePage(context.basic_set_[context.basic_set_.size()-2].PageId());
                            return;
                        }
                        RemoveFromInternalWithIndex(fa_page, fa_slot_num, txn);
                        //delete leaf_page
                        bpm_->DeletePage(now_page_id);
                    }
                    //refresh now_page and now_page_id
                    now_page = fa_page;
                    now_page_id = context.basic_set_[context.basic_set_.size() - 2].PageId();
                    context.basic_set_.pop_back();
                }
            }
            /*
            else{
                //if this page is a internal page
                auto internal_page = reinterpret_cast<InternalPage*>(now_page);
                if(internal_page->GetSize() >= ((internal_max_size_/2 > 1)?(internal_max_size_/2):2)){
                    //recursively check if father page needs to be updated
                    while(context.basic_set_.size() > 1){
                        bool is_leaf = now_page->IsLeafPage();
                        auto fa_page = context.basic_set_[context.basic_set_.size()-2].template AsMut<InternalPage>();
                        auto fa_slot_num = BinaryFind(fa_page, key);
                        if(fa_slot_num == 0){
                            break;
                        }
                        else if(fa_slot_num == 1){ 
                            if(is_leaf){
                                fa_page->SetKeyAt(1, leaf_page->KeyAt(0));
                                is_leaf = false;
                            }
                            else{
                                fa_page->SetKeyAt(1, now_internal_page->KeyAt(1));
                            }
                            now_internal_page = fa_page;
                            context.basic_set_.pop_back();
                        }
                        else{
                            if(is_leaf){
                                fa_page->SetKeyAt(fa_slot_num, leaf_page->KeyAt(0));
                                is_leaf = false;
                            }
                            else{
                                fa_page->SetKeyAt(fa_slot_num, now_internal_page->KeyAt(1));
                            }
                            break;
                        }
                    }
                }
                //merge
                if(context.basic_set_.size() == 1){
                    //if this is the root page
                    if(internal_page->GetSize() == 1){
                        //if the root page has only one key, delete it
                        // SetRootPageId(internal_page->ValueAt(0));
                        auto root_header_page = context.header_page_.value().template AsMut<BPlusTreeHeaderPage>();
                        root_header_page->root_page_id_ = INVALID_PAGE_ID;
                        bpm_->DeletePage(now_page_id);
                        return;
                    }
                    break;
                }
                //if this is not the root page
                // fa_guard = context.basic_set_[context.basic_set_.size()-2];
                // find left_brother(if exist) and try to redistribute
                // auto fa_page = context.basic_set_[context.basic_set_.size()-2].template AsMut<InternalPage>();
            }
            */
        }
        return;
    }

    /*****************************************************************************
     * INDEX ITERATOR
     *****************************************************************************/


    INDEX_TEMPLATE_ARGUMENTS
        auto BPLUSTREE_TYPE::BinaryFind(const LeafPage* leaf_page, const KeyType& key)
        ->  int
    {
        int l = 0;
        int r = leaf_page->GetSize() - 1;
        while (l < r)
        {
            int mid = (l + r + 1) >> 1;
            if (comparator_(leaf_page->KeyAt(mid), key) != 1)
            {
                l = mid;
            }
            else
            {
                r = mid - 1;
            }
        }
        if (r >= 0 && comparator_(leaf_page->KeyAt(r), key) != 0)
        {
            r = -1;
        }
        return r;
    }

    INDEX_TEMPLATE_ARGUMENTS
        auto BPLUSTREE_TYPE::BinaryFind(const InternalPage* internal_page,
            const KeyType& key)  ->  int
    {
        int l = 1;
        int r = internal_page->GetSize() - 1;
        while (l < r)
        {
            int mid = (l + r + 1) >> 1;
            if (comparator_(internal_page->KeyAt(mid), key) != 1)
            {
                l = mid;
            }
            else
            {
                r = mid - 1;
            }
        }

        if (r == -1 || comparator_(internal_page->KeyAt(r), key) == 1)
        {
            r = 0;
        }

        return r;
    }

    /*
     * Input parameter is void, find the leftmost leaf page first, then construct
     * index iterator
     * @return : index iterator
     */
    INDEX_TEMPLATE_ARGUMENTS
        auto BPLUSTREE_TYPE::Begin()  ->  INDEXITERATOR_TYPE
        //Just go left forever
    {
        ReadPageGuard head_guard = bpm_->FetchPageRead(header_page_id_);
        if (head_guard.template As<BPlusTreeHeaderPage>()->root_page_id_ == INVALID_PAGE_ID)
        {
            return End();
        }
        ReadPageGuard guard = bpm_->FetchPageRead(head_guard.As<BPlusTreeHeaderPage>()->root_page_id_);
        head_guard.Drop();

        auto tmp_page = guard.template As<BPlusTreePage>();
        while (!tmp_page->IsLeafPage())
        {
            int slot_num = 0;
            guard = bpm_->FetchPageRead(reinterpret_cast<const InternalPage*>(tmp_page)->ValueAt(slot_num));
            tmp_page = guard.template As<BPlusTreePage>();
        }
        int slot_num = 0;
        if (slot_num != -1)
        {
            return INDEXITERATOR_TYPE(bpm_, guard.PageId(), 0);
        }
        return End();
    }


    /*
     * Input parameter is low key, find the leaf page that contains the input key
     * first, then construct index iterator
     * @return : index iterator
     */
    INDEX_TEMPLATE_ARGUMENTS
        auto BPLUSTREE_TYPE::Begin(const KeyType& key)  ->  INDEXITERATOR_TYPE
    {
        ReadPageGuard head_guard = bpm_->FetchPageRead(header_page_id_);

        if (head_guard.template As<BPlusTreeHeaderPage>()->root_page_id_ == INVALID_PAGE_ID)
        {
            return End();
        }
        ReadPageGuard guard = bpm_->FetchPageRead(head_guard.As<BPlusTreeHeaderPage>()->root_page_id_);
        head_guard.Drop();
        auto tmp_page = guard.template As<BPlusTreePage>();
        while (!tmp_page->IsLeafPage())
        {
            auto internal = reinterpret_cast<const InternalPage*>(tmp_page);
            int slot_num = BinaryFind(internal, key);
            if (slot_num == -1)
            {
                return End();
            }
            guard = bpm_->FetchPageRead(reinterpret_cast<const InternalPage*>(tmp_page)->ValueAt(slot_num));
            tmp_page = guard.template As<BPlusTreePage>();
        }
        auto* leaf_page = reinterpret_cast<const LeafPage*>(tmp_page);

        int slot_num = BinaryFind(leaf_page, key);
        if (slot_num != -1)
        {
            return INDEXITERATOR_TYPE(bpm_, guard.PageId(), slot_num);
        }
        return End();
    }

    /*
     * Input parameter is void, construct an index iterator representing the end
     * of the key/value pair in the leaf node
     * @return : index iterator
     */
    INDEX_TEMPLATE_ARGUMENTS
        auto BPLUSTREE_TYPE::End()  ->  INDEXITERATOR_TYPE
    {
        return INDEXITERATOR_TYPE(bpm_, -1, -1);
    }

    /**
     * @return Page id of the root of this tree
     */
    INDEX_TEMPLATE_ARGUMENTS
        auto BPLUSTREE_TYPE::GetRootPageId()  ->  page_id_t
    {
        ReadPageGuard guard = bpm_->FetchPageRead(header_page_id_);
        auto root_header_page = guard.template As<BPlusTreeHeaderPage>();
        page_id_t root_page_id = root_header_page->root_page_id_;
        return root_page_id;
    }

    /*****************************************************************************
     * UTILITIES AND DEBUG
     *****************************************************************************/

     /*
      * This method is used for test only
      * Read data from file and insert one by one
      */
    INDEX_TEMPLATE_ARGUMENTS
        void BPLUSTREE_TYPE::InsertFromFile(const std::string& file_name,
            Transaction* txn)
    {
        int64_t key;
        std::ifstream input(file_name);
        while (input >> key)
        {
            KeyType index_key;
            index_key.SetFromInteger(key);
            RID rid(key);
            Insert(index_key, rid, txn);
        }
    }
    /*
     * This method is used for test only
     * Read data from file and remove one by one
     */
    INDEX_TEMPLATE_ARGUMENTS
        void BPLUSTREE_TYPE::RemoveFromFile(const std::string& file_name,
            Transaction* txn)
    {
        int64_t key;
        std::ifstream input(file_name);
        while (input >> key)
        {
            KeyType index_key;
            index_key.SetFromInteger(key);
            Remove(index_key, txn);
        }
    }

    /*
     * This method is used for test only
     * Read data from file and insert/remove one by one
     */
    INDEX_TEMPLATE_ARGUMENTS
        void BPLUSTREE_TYPE::BatchOpsFromFile(const std::string& file_name,
            Transaction* txn)
    {
        int64_t key;
        char instruction;
        std::ifstream input(file_name);
        while (input)
        {
            input >> instruction >> key;
            RID rid(key);
            KeyType index_key;
            index_key.SetFromInteger(key);
            switch (instruction)
            {
            case 'i':
                Insert(index_key, rid, txn);
                break;
            case 'd':
                Remove(index_key, txn);
                break;
            default:
                break;
            }
        }
    }

    INDEX_TEMPLATE_ARGUMENTS
        void BPLUSTREE_TYPE::Print(BufferPoolManager* bpm)
    {
        auto root_page_id = GetRootPageId();
        auto guard = bpm->FetchPageBasic(root_page_id);
        PrintTree(guard.PageId(), guard.template As<BPlusTreePage>());
    }

    INDEX_TEMPLATE_ARGUMENTS
        void BPLUSTREE_TYPE::PrintTree(page_id_t page_id, const BPlusTreePage* page)
    {
        if (page->IsLeafPage())
        {
            auto* leaf = reinterpret_cast<const LeafPage*>(page);
            std::cout << "Leaf Page: " << page_id << "\tNext: " << leaf->GetNextPageId() << std::endl;

            // Print the contents of the leaf page.
            std::cout << "Contents: ";
            for (int i = 0; i < leaf->GetSize(); i++)
            {
                std::cout << leaf->KeyAt(i);
                if ((i + 1) < leaf->GetSize())
                {
                    std::cout << ", ";
                }
            }
            std::cout << std::endl;
            std::cout << std::endl;
        }
        else
        {
            auto* internal = reinterpret_cast<const InternalPage*>(page);
            std::cout << "Internal Page: " << page_id << std::endl;

            // Print the contents of the internal page.
            std::cout << "Contents: ";
            for (int i = 0; i < internal->GetSize(); i++)
            {
                std::cout << internal->KeyAt(i) << ": " << internal->ValueAt(i);
                if ((i + 1) < internal->GetSize())
                {
                    std::cout << ", ";
                }
            }
            std::cout << std::endl;
            std::cout << std::endl;
            for (int i = 0; i < internal->GetSize(); i++)
            {
                auto guard = bpm_->FetchPageBasic(internal->ValueAt(i));
                PrintTree(guard.PageId(), guard.template As<BPlusTreePage>());
            }
        }
    }

    /**
     * This method is used for debug only, You don't need to modify
     */
    INDEX_TEMPLATE_ARGUMENTS
        void BPLUSTREE_TYPE::Draw(BufferPoolManager* bpm, const std::string& outf)
    {
        if (IsEmpty())
        {
            LOG_WARN("Drawing an empty tree");
            return;
        }

        std::ofstream out(outf);
        out << "digraph G {" << std::endl;
        auto root_page_id = GetRootPageId();
        auto guard = bpm->FetchPageBasic(root_page_id);
        ToGraph(guard.PageId(), guard.template As<BPlusTreePage>(), out);
        out << "}" << std::endl;
        out.close();
    }

    /**
     * This method is used for debug only, You don't need to modify
     */
    INDEX_TEMPLATE_ARGUMENTS
        void BPLUSTREE_TYPE::ToGraph(page_id_t page_id, const BPlusTreePage* page,
            std::ofstream& out)
    {
        std::string leaf_prefix("LEAF_");
        std::string internal_prefix("INT_");
        if (page->IsLeafPage())
        {
            auto* leaf = reinterpret_cast<const LeafPage*>(page);
            // Print node name
            out << leaf_prefix << page_id;
            // Print node properties
            out << "[shape=plain color=green ";
            // Print data of the node
            out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" "
                "CELLPADDING=\"4\">\n";
            // Print data
            out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">P=" << page_id
                << "</TD></TR>\n";
            out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">"
                << "max_size=" << leaf->GetMaxSize()
                << ",min_size=" << leaf->GetMinSize() << ",size=" << leaf->GetSize()
                << "</TD></TR>\n";
            out << "<TR>";
            for (int i = 0; i < leaf->GetSize(); i++)
            {
                out << "<TD>" << leaf->KeyAt(i) << "</TD>\n";
            }
            out << "</TR>";
            // Print table end
            out << "</TABLE>>];\n";
            // Print Leaf node link if there is a next page
            if (leaf->GetNextPageId() != INVALID_PAGE_ID)
            {
                out << leaf_prefix << page_id << "   ->   " << leaf_prefix
                    << leaf->GetNextPageId() << ";\n";
                out << "{rank=same " << leaf_prefix << page_id << " " << leaf_prefix
                    << leaf->GetNextPageId() << "};\n";
            }
        }
        else
        {
            auto* inner = reinterpret_cast<const InternalPage*>(page);
            // Print node name
            out << internal_prefix << page_id;
            // Print node properties
            out << "[shape=plain color=pink ";  // why not?
            // Print data of the node
            out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" "
                "CELLPADDING=\"4\">\n";
            // Print data
            out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">P=" << page_id
                << "</TD></TR>\n";
            out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">"
                << "max_size=" << inner->GetMaxSize()
                << ",min_size=" << inner->GetMinSize() << ",size=" << inner->GetSize()
                << "</TD></TR>\n";
            out << "<TR>";
            for (int i = 0; i < inner->GetSize(); i++)
            {
                out << "<TD PORT=\"p" << inner->ValueAt(i) << "\">";
                // if (i > 0) {
                out << inner->KeyAt(i) << "  " << inner->ValueAt(i);
                // } else {
                // out << inner  ->  ValueAt(0);
                // }
                out << "</TD>\n";
            }
            out << "</TR>";
            // Print table end
            out << "</TABLE>>];\n";
            // Print leaves
            for (int i = 0; i < inner->GetSize(); i++)
            {
                auto child_guard = bpm_->FetchPageBasic(inner->ValueAt(i));
                auto child_page = child_guard.template As<BPlusTreePage>();
                ToGraph(child_guard.PageId(), child_page, out);
                if (i > 0)
                {
                    auto sibling_guard = bpm_->FetchPageBasic(inner->ValueAt(i - 1));
                    auto sibling_page = sibling_guard.template As<BPlusTreePage>();
                    if (!sibling_page->IsLeafPage() && !child_page->IsLeafPage())
                    {
                        out << "{rank=same " << internal_prefix << sibling_guard.PageId()
                            << " " << internal_prefix << child_guard.PageId() << "};\n";
                    }
                }
                out << internal_prefix << page_id << ":p" << child_guard.PageId()
                    << "   ->   ";
                if (child_page->IsLeafPage())
                {
                    out << leaf_prefix << child_guard.PageId() << ";\n";
                }
                else
                {
                    out << internal_prefix << child_guard.PageId() << ";\n";
                }
            }
        }
    }

    INDEX_TEMPLATE_ARGUMENTS
        auto BPLUSTREE_TYPE::DrawBPlusTree()  ->  std::string
    {
        if (IsEmpty())
        {
            return "()";
        }

        PrintableBPlusTree p_root = ToPrintableBPlusTree(GetRootPageId());
        std::ostringstream out_buf;
        p_root.Print(out_buf);

        return out_buf.str();
    }

    INDEX_TEMPLATE_ARGUMENTS
        auto BPLUSTREE_TYPE::ToPrintableBPlusTree(page_id_t root_id)
        ->  PrintableBPlusTree
    {
        auto root_page_guard = bpm_->FetchPageBasic(root_id);
        auto root_page = root_page_guard.template As<BPlusTreePage>();
        PrintableBPlusTree proot;

        if (root_page->IsLeafPage())
        {
            // std::cout<<"leaf!"<<std::endl;
            auto leaf_page = root_page_guard.template As<LeafPage>();
            proot.keys_ = leaf_page->ToString();
            proot.size_ = proot.keys_.size() + 4;  // 4 more spaces for indent

            return proot;
        }

        // draw internal page
      //   std::cout<<"internal!"<<std::endl;
        auto internal_page = root_page_guard.template As<InternalPage>();
        proot.keys_ = internal_page->ToString();
        proot.size_ = 0;
        for (int i = 0; i < internal_page->GetSize(); i++)
        {
            page_id_t child_id = internal_page->ValueAt(i);
            // std::cout<<"child_id="<<child_id<<std::endl;
            PrintableBPlusTree child_node = ToPrintableBPlusTree(child_id);
            proot.size_ += child_node.size_;
            proot.children_.push_back(child_node);
        }

        return proot;
    }

    template class BPlusTree<GenericKey<4>, RID, GenericComparator<4>>;

    template class BPlusTree<GenericKey<8>, RID, GenericComparator<8>>;

    template class BPlusTree<GenericKey<16>, RID, GenericComparator<16>>;

    template class BPlusTree<GenericKey<32>, RID, GenericComparator<32>>;

    template class BPlusTree<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub