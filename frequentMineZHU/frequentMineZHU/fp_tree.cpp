/*
   Author:  Jianfei Zhu  
            Concordia University
   Date:    Sep. 9, 2004

Copyright (c) 2004, Concordia University, Montreal, Canada
All rights reserved.

Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the following conditions are met:

   - Redistributions of source code must retain the above copyright notice,
       this list of conditions and the following disclaimer.
   - Redistributions in binary form must reproduce the above copyright
       notice, this list of conditions and the following disclaimer in the
       documentation and/or other materials provided with the distribution.
   - Neither the name of Concordia University nor the names of its
       contributors may be used to endorse or promote products derived from
       this software without specific prior written permission.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE
LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF
THE POSSIBILITY OF SUCH DAMAGE.

*/

#include <stdlib.h>
#include <assert.h>
#include <math.h>
#include "buffer.h"
#include "common.h"

template <class T> void swap(T* k, T* j)
{
	T temp;
	temp = *j;
	*j = *k;
	*k = temp;
}

int findpivot(const int& i, const int& j) {return (i+j)/2;}

int partition(int* array, int* temp, int l, int r, int pivot)
{
	do {
		while (array[++l] > pivot);
		while (r && array[--r] < pivot);
		swap(array+l, array+r);
		swap(temp+l, temp+r);
	}while (l < r);
	swap(array + l, array + r);
	swap(temp + l, temp + r);
	return l;
}

//Bug! Dec. 4, 2003
void inssort(int* array, int* temp, int i, int j)
{
	for (int k=i+1; k<=j; k++)
		for (int m=k; (m>i) && (array[m] > array[m-1]); m--)
		{
			swap(array+m, array+m-1);
		    swap(temp+m, temp+m-1);
		}
}

void sort(int *array, int* temp, int i, int j)
{
	if(j-i < SORTHRESH) inssort(array, temp, i, j);
	else
	{
		int pivotindex = findpivot(i, j);
		swap(array+pivotindex, array+j);
		swap(temp+pivotindex, temp+j);
		int k = partition(array, temp, i-1, j, array[j]);
		swap(array+k, array+j);
		swap(temp+k, temp+j);
		if((k-i)>1) sort(array, temp, i, k-1);
		if((j-k)>1) sort(array, temp, k+1, j);
	}
}

void update_mfi_trees(int treeno)
{
	int i, j;
	for(i=0; i<treeno; i++)
	{
		for(j = list->top-1; j >mfitrees[i]->posi+1; j--)
			current_fi[mfitrees[i]->order[list->FS[j]]] = true;

		mfitrees[i]->insert(current_fi, list->top-1-mfitrees[i]->posi-1);
	}
}

void update_cfi_trees(int treeno, int Count)
{
	int i, j;
	for(i=0; i<treeno; i++)
	{
		for(j = list->top-1; j >cfitrees[i]->posi+1; j--)
			current_fi[cfitrees[i]->order[list->FS[j]]] = true;

		if(list->top-1-cfitrees[i]->posi-1>0)
                    cfitrees[i]->insert(current_fi, Count, list->top-1-cfitrees[i]->posi-1);
	}
}

stack::stack(int length, bool close)
{
	top= 0; 
	FS = new int[length];
	if(close)
		counts = new int[length];
	else
		counts = NULL;
}

stack::~stack()
{
	delete []FS;
}

void stack::insert(FI_tree* fptree)
{
	for(Fnode* node=fptree->Root->leftchild; node!=NULL; node=node->leftchild)
	{
		FS[top]=node->itemname; 
		top++;
	}
}

void CFI_tree::init(memory* close_buf, FI_tree* fp_tree, CFI_tree* old_LClose, Cnode* node, int Posi)
{	
	int i;

	CloseNo=0;

	order = fp_tree->order;
	table = fp_tree->table;
	itemno = fp_tree->itemno;
	posi = Posi;
	Close_buf = close_buf;
	Root = (Cnode*)Close_buf->newbuf(1, sizeof(Cnode));
	Root->init(NULL, -1, 0, 0);
	head = (Cnode**)Close_buf->newbuf(itemno, sizeof(Cnode*));
	for(i=0; i<itemno; i++)
		head[i] = NULL;
	if(old_LClose && node)		
	{
		int has, current;
		bool* origin;
		Cnode* link, *parent;
		int* old_order;

		old_order = old_LClose->order;
		current = old_order[node->itemname];

		origin = new bool[current];
		assert(origin!=NULL);

		for(i=0; i<current; i++) origin[i]=false;
		
		for(link=node; link!=NULL; link=link->next)
		{	
			has=0;
			for(parent=link->par; parent->itemname!=-1; parent=parent->par)
				if(order[parent->itemname]!=-1)
				{
					origin[order[parent->itemname]]=true;
					has++;
				}

			if(has)
				insert(origin, link->count, has);
		}

		delete []origin;
	}
}

bool CFI_tree::is_subset(int Count)const
{
	int i, *FS;
	Cnode* temp;

	FS = list->FS;
	if(list->top==posi+2&&head[order[FS[posi+1]]])
	{
		for(temp=head[order[FS[posi+1]]];temp!=NULL; temp=temp->next)
			if(temp->count >= Count)return true;
		return false;
	}

	Cnode* temp2;
	for(temp=head[order[FS[posi+1]]];temp!=NULL; temp=temp->next)
	{
		if(temp->level<list->top-1-posi || temp->count<Count)continue;
		temp2=temp->par;
		for(i=list->top-1; i>=posi+2; i--)
		{
            for(; temp2->itemname!=-1 && order[temp2->itemname]>order[FS[i]]&&temp2->level>=i-posi-1; temp2=temp2->par);
      		if(temp2->itemname!=FS[i])break;
			if(i==posi+2)return true;
		}
	}

	return false;
}

void CFI_tree::insert(bool* origin, int Count, int has)
{
	int i=0, j=1, k=0;
	Cnode* root=Root, *temp=NULL;

	if(posi==-1)CloseNo++;

	while(k<has)
	{
		if(origin[i])
		{
			origin[i]=false;
			k++;
			temp = root->leftchild;
			if(temp == NULL)break;
			for(; temp->rightsibling!=NULL; temp=temp->rightsibling)
				if(temp->itemname==table[i])break;
			if(temp->itemname==table[i])	    
			{
				if(temp->count <Count)temp->count = Count;
				root=temp; 
			}else break;          //temp->rightsibling == NULL
		}
		i++;
	}

	if(k==has && root==temp)return;

	origin[i] = true;
	for(j=i; k<=has; j++)
		if(origin[j])
		{
			origin[j] = false;
			root=root->append(this, temp, table[j], k, Count);
			k++;
		}
}

void CFI_tree::insert(int* mfi, int start, int end, int Count)  //compact info
{
	int i, j=1, k=0;
	Cnode* root=Root, *temp=NULL;

	if(posi==-1)CloseNo++;

	if(start==end || start+1==end)
		return;
//Modified on Feb. 10, 2004
	
	i = start+1;

	while(i<end)
	{
		k++;
		temp = root->leftchild;
		if(temp == NULL)break;
		for(; temp->rightsibling!=NULL; temp=temp->rightsibling)
			if(temp->itemname==mfi[i])break;
		if(temp->itemname==mfi[i])		    //temp->rightsibling == NULL
		{
			if(temp->count <Count)temp->count = Count;
			root=temp; 
		}else break;
		i++;
	}

//Modified on Feb. 10, 2004
	if(i!=end)
		for(j=i; j<end; j++)
		{
			root=root->append(this, temp, mfi[j], k, Count);
			k++;
		}
}
//Patch  Dec. 4

void CFI_tree::order_FS(int* FS, int start, int end)
{     
        int i;
                
	for(i = start; i<end; i++)
		if(order[FS[i]]>order[FS[i+1]])break;
	if(i==end)return;

	for (int k=start+1; k<=end; k++)
		for (int m=k; (m>start) && (order[FS[m]] < order[FS[m-1]]); m--)
			swap(FS+m, FS+m-1);
}

bool CFI_tree::generate_close(int new_item_no, int Count, FSout* fout)
{
	int i, whole, temp = Count;
	
	if(list->top>1)
		temp=list->counts[list->top-2];
	
	whole = list->top+new_item_no;
	for(i=list->top; i<whole && list->counts[i]==Count; i++);
	list->top = i;

	ITlen[i-1]++;
	if(fout)
		fout->printSet(i, list->FS, Count);

//	insert(list->FS, posi+1, i, Count);
//	update_cfi_trees(posi+1, Count);
	update_cfi_trees(posi+2, Count);  //Modified on Oct. 10, 2003

	if(i==whole && temp == Count)
		return true;	

	while(i<whole)
	{
		Count = list->counts[i];
		for(; i<whole && list->counts[i]==Count; i++);
		list->top = i;

//Patch Dec. 4
		order_FS(list->FS, posi+2, i-1);
		if(!is_subset(Count))
		{
			ITlen[i-1]++;
			if(fout)
				fout->printSet(i, list->FS, Count);
//			insert(list->FS, posi+1, i, Count);
//			update_cfi_trees(posi+1, Count);
			update_cfi_trees(posi+2, Count);   //Modified on Oct. 10, 2003
		}else{
		}
	}

	return false;
}

void MFI_tree::init(memory* max_buf, FI_tree* fp_tree, MFI_tree* old_LMFI, Mnode* node, int Posi)
{	
	int i;

	MFSNo=0;

	order = fp_tree->order;
	table = fp_tree->table;
	itemno = fp_tree->itemno;
	posi = Posi;
	Max_buf = max_buf;
	Root = (Mnode*)Max_buf->newbuf(1, sizeof(Mnode));
	Root->init(NULL, -1, 0);
	head = (Mnode**)Max_buf->newbuf(itemno, sizeof(Mnode*));
	for(i=0; i<itemno; i++)
		head[i] = NULL;

	if(old_LMFI && node)
	{
		int has, current;
		bool* origin;
		Mnode* link, *parent;
		int* old_order;

		old_order = old_LMFI->order;
		current = old_order[node->itemname];

		origin = new bool[current];
		assert(origin!=NULL);

		for(i=0; i<current; i++) origin[i]=false;
		
		for(link=node; link!=NULL; link=link->next)
		{	
			has=0;
			for(parent=link->par; parent->itemname!=-1; parent=parent->par)
				if(order[parent->itemname]!=-1)
				{
					origin[order[parent->itemname]]=true;
					has++;
				}

			if(has)
				insert(origin, has);
		}

		delete []origin;
	}
}

bool MFI_tree::is_subset()
{
	int i;
	int *FS;

	FS = list->FS;
	if(list->top==posi+2&&head[order[FS[posi+1]]])return true;

	Mnode* temp, *temp2;
	for(temp=head[order[FS[posi+1]]];temp!=NULL; temp=temp->next)
	{
		if(temp->level<list->top-1-posi)continue;
		temp2=temp->par;
		for(i=list->top-1; i>=posi+2; i--)
		{
            for(; temp2->itemname!=-1 && order[temp2->itemname]>order[FS[i]]&&temp2->level>=i-posi-1; temp2=temp2->par);
      		if(temp2->itemname!=FS[i])break;
			if(i==posi+2)return true;
		}
	}

	return false;
}

void MFI_tree::insert(bool* origin, int has)
{
	int i=1, j=1, k=0;
	
	if(posi==-1)MFSNo++;

	Mnode* root=Root;
	Mnode* temp=NULL;
	i=0;
	while(k<has)
	{
		if(origin[i])
		{
			origin[i]=false;
			k++;
			temp = root->leftchild;
			if(temp == NULL)break;
			for(; temp->rightsibling!=NULL; temp=temp->rightsibling)
				if(temp->itemname==table[i])break;
			if(temp->itemname==table[i])root=temp;    //temp->rightsibling == NULL
			else break;
//			if(k==has)return;
		}
		i++;
	}
	if(k==has && root==temp)return;

	origin[i] = true;
	for(j=i; k<=has; j++)
		if(origin[j])
		{
			origin[j] = false;
			root=root->append(this, temp, table[j], k);
			k++;
		}
}

void MFI_tree::insert(int* mfi, int start, int end)  //compact info
{
	int i, j=1, k=0;
	Mnode* root=Root, *temp=NULL;

	if(posi==-1)MFSNo++;

//Modified on Feb. 10, 2004
	if(start==end) return;

	i = start+1;
	while(i<end)
	{
		k++;
		temp = root->leftchild;
		if(temp == NULL)break;
		for(; temp->rightsibling!=NULL; temp=temp->rightsibling)
			if(temp->itemname==mfi[i])break;
		if(temp->itemname==mfi[i])root=temp;    //temp->rightsibling == NULL
		else break;
		i++;
	}

//Modified on Feb. 10, 2004
	if(i!=end)
	{ 
		for(j=i; j<end; j++)
		{
			root=root->append(this, temp, mfi[j], k);
			k++;
		}
	}
}

void FI_tree::init(int old_itemno, int new_itemno)
{
	int i;

	Root = (Fnode*)fp_buf->newbuf(1, sizeof(Fnode));
	Root->init(NULL, -1, 0);
//	Root = new Fnode(-1, 0, NULL);
	if(old_itemno!=-1)
	{
		order = (int*)fp_buf->newbuf(ITEM_NO, sizeof(int));
		count = (int*)fp_buf->newbuf(old_itemno, sizeof(int));
		table = (int*)fp_buf->newbuf(old_itemno, sizeof(int));
		for (i=0; i<old_itemno; i++)
		{
			order[i]=-1;
			count[i] = 0;
			table[i] = i;
		}
		for (; i<ITEM_NO; i++)
			order[i]=-1;
	}

	itemno = new_itemno;
	if(new_itemno!=0)
		head = (Fnode**)fp_buf->newbuf(itemno, sizeof(Fnode*));
}

void FI_tree::scan1_DB(Data* fdat)
{
	int i,j;
	int net_itemno=0;
	int *counts;

	counts= new int[ITEM_NO];
	assert(counts!=NULL);

	for(i=0; i<ITEM_NO; i++)
		counts[i] = 0;

	Transaction *Tran = new Transaction;
	assert(Tran!=NULL);

	while(Tran = fdat->getNextTransaction(Tran))
	{	
		for(int i=0; i<Tran->length; i++) 
		{
			if(Tran->t[i]>=ITEM_NO)
			{
				ITEM_NO = 2*Tran->t[i];
				int* temp = new int[2*Tran->t[i]];
				for(j=0; j<=net_itemno; j++)
					temp[j] = counts[j];
				for(; j<ITEM_NO; j++)
					temp[j] = 0;
				delete []counts;
				counts=temp;
				net_itemno=Tran->t[i];
			}else
				if(net_itemno<Tran->t[i])
					net_itemno = Tran->t[i];
			counts[Tran->t[i]]++;
		}
		TRANSACTION_NO++;
	}

	ITEM_NO = net_itemno+1;

//	cout << "The dataset contains " << TRANSACTION_NO << " transactions and "
//       << "the largest item is " << net_itemno << endl;

//	THRESHOLD = ceil (TRANSACTION_NO/100000.0 * THRESHOLD);
//	cout << "The minimum support is "<<THRESHOLD<<endl;

	order = (int*)fp_buf->newbuf(ITEM_NO, sizeof(int));
	table = (int*)fp_buf->newbuf(ITEM_NO, sizeof(int));
	count = (int*)fp_buf->newbuf(ITEM_NO, sizeof(int));
	for (i=0; i<ITEM_NO; i++)
	{
		order[i]=-1;
		count[i] = counts[i];
		table[i] = i;
	}

	sort(count, table, 0, ITEM_NO-1);
	
	for (i =0; i<ITEM_NO&&count[i] >= THRESHOLD; i++);

	itemno = i;

	for (j=0; j<itemno; j++)
	{
		count[j]=counts[table[j]];  
		order[table[j]]=j;
	}

//	for(int k=0; k<ITEM_NO; k++)cout<<order[k]<<"  "; cout<<endl;
//	for( k=0; k<itemno; k++)cout<<table[k]<<"  "; cout<<endl;

	head = (Fnode**)fp_buf->newbuf(itemno, sizeof(Fnode*));

	if(itemno>SUDDEN+5)
	{
		array = new int*[itemno-1-SUDDEN];
		for(i=0; i<itemno-1-SUDDEN; i++)
		{
			array[i] = new int[itemno-1-i];
			for(j=0; j<itemno-1-i; j++)
				array[i][j] = 0;
		}
	}else array = NULL;

// The following is for the case when ITEM_NO is very big and fptree->itemno is very small
	order_item = new int[itemno];
	item_order = new int[ITEM_NO];
	for(i=0; i<itemno; i++)
	{
		head[i] = (Fnode*)fp_buf->newbuf(1, sizeof(Fnode));
		head[i]->init(NULL, i, count[i]);		

		order_item[i]=table[i];
		table[i]=i;
		item_order[i] = order[i];
		order[i]=i;
	}
	for(;i<ITEM_NO; i++)
	{
		item_order[i] = order[i];
		order[i]=-1;
	}
	ITEM_NO = itemno;

	delete []counts;
	delete Tran;
}
	
void FI_tree::insert(int* compact, int counts, int current)
{
	Fnode* child = Root;
	Fnode* temp, *temp1=NULL;
	int i=0;

	while(i<current)
	{
		for(temp=child->leftchild; temp!=NULL; temp = temp->rightsibling)
		{
			if(temp->itemname==table[compact[i]])break;
			if(temp->rightsibling==NULL)temp1 = temp;
		}

		if(!temp)break;

		temp->count+=counts;
		child=temp;
		i++;
	}
	while(i<current)
	{   
		child = child->append(this, temp1, table[compact[i]], counts);
		bran[i]++;
		i++;
	}
}

void FI_tree::cal_level_25()
{
	int i, total_25=0, total_50=0, total_bran=0, maxlen=0;

	for(i=0; i<this->itemno && bran[i]!=0; i++);
	maxlen =i;
	for(i=0; i<int(maxlen*0.25); i++)
		total_25 +=bran[i];
	total_50 = total_25;
	for(i=int(maxlen*0.25); i<this->itemno*0.5; i++)
		total_50 +=bran[i];
	for(i=0; i<this->itemno && bran[i]!=0; i++)
	{
//		cout<<i<<" " <<bran[i]<<endl;
		total_bran+=bran[i];
		bran[i]=0;
	}
	level_25 = (double)total_25/total_bran*100;
//	cout<<"First 25% levels: "<<(double)total_25/total_bran*100<<"%  "<<"50% levels: "<<(double)total_50/total_bran*100<<"%"<<endl;
}

void FI_tree::fill_count(int* origin, int support)
{
	int i, j=0;
	for(i=0; i<itemno; i++)
	{
		if(origin[i]!=-1)
		{
			compact[j++]=i;
			origin[i] = -1;
		}
	}

	if(array)
	{
		int comp_len = j;
		for(i=comp_len-1; i>0 && compact[i]>SUDDEN; i--)
			for(j=i-1; j>=0; j--)
				array[itemno-1-compact[i]][compact[i]-compact[j]-1]+=support;
	}
}

void FI_tree::scan2_DB(Data *fdat)
{
	int i, j, has;
	int* origin, *buffer=new int;
	Transaction *Tran = new Transaction;
	origin = new int[ITEM_NO];
	assert(Tran!=NULL&&origin!=NULL && buffer!=NULL);

	for(j=0; j<ITEM_NO; j++) origin[j]=-1;

	for(i=0; i<TRANSACTION_NO; i++)
	{
		Tran = fdat->getNextTransaction(Tran);
		has=0;
		for(int j=0; j<Tran->length; j++) 
		{
			if(item_order[Tran->t[j]]!=-1)
			{
				has++;
//				if(origin[item_order[Tran->t[j]]]!=-1)
//					has--;         // in transaction, items are not supposed to appear more than twice
				origin[item_order[Tran->t[j]]] = item_order[Tran->t[j]];
			}
		}
		if(has){
			fill_count(origin, 1);
			insert(compact, 1, has);
		}
	}

	cal_level_25();

	delete []origin;
	delete buffer;
	delete Tran;
}

void FI_tree::scan1_DB(FI_tree* old_tree)
{
	int i, j;
	int* old_order = old_tree->order;

	for(i=0; i< itemno; i++)
	{
		count[i]=supp[old_order[list->FS[i+list->top-itemno]]];
		table[i]=list->FS[i+list->top-itemno];
		supp[old_order[list->FS[i+list->top-itemno]]]=0;
	}

	sort(count, table, 0, itemno-1);

	for(i=0; i<itemno; i++)
	{
		order[table[i]]=i;
		head[i] = (Fnode*)fp_buf->newbuf(1, sizeof(Fnode));
		head[i]->init(NULL, table[i], count[i]);		
	}

	if(itemno > SUDDEN+5 && old_tree->level_25 > SWITCH)
	{
		array = (int**)fp_buf->newbuf(itemno-1-SUDDEN, sizeof(int*));
		for(i=0; i<itemno-1-SUDDEN; i++)
		{
			array[i] = (int*)fp_buf->newbuf(itemno-1-i, sizeof(int));
			for(j=0; j<itemno-1-i; j++)
				array[i][j] = 0;
		}
	}else array = NULL;
}

void FI_tree::scan2_DB(FI_tree* old_tree, Fnode* node)
{
	int i, has, current;
	int* origin;
	Fnode* link, *parent;
	int* old_order;
	
	old_order = old_tree->order;
	current = old_order[node->itemname];

	origin = new int[current];
	assert(origin!=NULL);

	for(i=0; i<current; i++) origin[i]=-1;
	
	for(link=node->next; link!=NULL; link=link->next)
	{	
		has=0;
		for(parent=link->par; parent->itemname!=-1; parent=parent->par)
			if(order[parent->itemname]!=-1)
			{
				origin[order[parent->itemname]]=parent->itemname;
				has++;
			}

		if(has)
		{
			fill_count(origin, link->count);
			insert(compact, link->count, has);
		}
	}

	cal_level_25();

	delete []origin;
}

void FI_tree::powerset(int*prefix, int prefixlen, int* items, int current, int itlen, FSout* fout )const
{
	if(current==itlen)
	{
		if(prefixlen!=0)
		{	
			ITlen[list->top+prefixlen-1]++;
			if(fout)
			{
				fout->printset(list->top, list->FS);
				fout->printSet(prefixlen, prefix, this->head[order[prefix[prefixlen-1]]]->count);
			}
		}
	}else{
		current++;
		powerset(prefix, prefixlen, items, current, itlen, fout);
		current--;
		prefix[prefixlen++]=items[current++];
		powerset(prefix, prefixlen, items, current, itlen, fout);
	}
}	

void FI_tree::generate_all(int new_item_no, FSout* fout)const
{
	powerset(prefix, 0, list->FS, list->top, list->top+new_item_no, fout);
}

bool FI_tree::Single_path(bool close)const
{
	Fnode* node;

	for(node=Root->leftchild; node!=NULL; node=node->leftchild)
		if(node->rightsibling!=NULL)return false;

	if(close)
		for(node=Root->leftchild; node!=NULL; node=node->leftchild)
		{
			list->FS[list->top] = node->itemname;
			list->counts[list->top++] = node->count;
		}

	return true;
}

memory* FI_tree::allocate_buf(int sequence, int iteration, int i)
				//	power2[i] is the smallest block size for memory
{
	memory* buf;				
	int blocks=60;
	int j=0;
	if(itemno<=100)j=sequence/10;
	else if(itemno>100 && itemno<=500)j=sequence/50;
	if(itemno>500)j = sequence/100;

	switch(iteration){
	case -1:	
		if(j<=4)
			buf=new memory(blocks, power2[i+j], power2[i+j+1], 2);
		else
			buf=new memory(blocks, power2[i+5], power2[i+6], 2);
		break;
	case 0:	
		if(j<=3)
			buf=new memory(blocks, power2[i+j], power2[i+j+1], 2);
		else
			buf=new memory(blocks, power2[i+4], power2[i+5], 2);
		break;
	case 1:	
		if(j<=2)
			buf=new memory(blocks, power2[i+j], power2[i+j+1], 2);
		else
			buf=new memory(blocks, power2[i+3], power2[i+4], 2);
		break;
	case 2:	
		if(j<=1)
			buf=new memory(blocks, power2[i+j], power2[i+j+1], 2);
		else
			buf=new memory(blocks, power2[i+2], power2[i+3], 2);
		break;
	case 3:	
		if(j<=0)
			buf=new memory(blocks, power2[i+j], power2[i+j+1], 2);
		else
			buf=new memory(blocks, power2[i+1], power2[i+2], 2);
		break;
	default:
		buf=new memory(blocks, power2[i], power2[i+1], 2);
	}
	return buf;
}

int FI_tree::conditional_pattern_base(Fnode* node, bool close)const
{
	Fnode* temp, *parent;
	int i;

	for(temp=node->next; temp!=NULL; temp=temp->next)
	{
		parent=temp->par;
		for(; parent->itemname!=-1;parent=parent->par)
			supp[order[parent->itemname]]+=temp->count;
	}

	int k=0;
	for(i=0; i<order[node->itemname]; i++)
	{
		if(supp[i]>=THRESHOLD)
		{
			k++;
			list->FS[list->top++]=table[i];
			if(close)list->counts[list->top-1] = supp[i];
		}else
			supp[i] = 0;
	}

	return k;
}

int FI_tree::conditional_pattern_base(int itemname, bool close)const
{
	int i, k=0, item = itemno-1-order[itemname];
	for(i=itemno-2-item; i>=0;i--)
		if(array[item][i]>=THRESHOLD)
		{
			k++;
			list->FS[list->top++]=table[itemno-2-item-i];
			supp[itemno-2-item-i]=array[item][i];
			if(close)list->counts[list->top-1] = array[item][i];
		}
	return k;
}

int FI_tree::FP_growth(FSout* fout)
{
	int i, sequence, current, new_item_no, listlen;
	int MC=0;			//markcount for memory
	unsigned int MR=0;	//markrest for memory
	char* MB;			//markbuf for memory

	Fnode* Current;

	for(sequence=itemno-1; sequence>=0; sequence--)
	{
		Current=head[sequence];
		current=head[sequence]->itemname;
		list->FS[list->top++]=head[sequence]->itemname;
		listlen = list->top;

		ITlen[list->top-1]++;
		if(fout)
			fout->printSet(list->top, list->FS, this->head[sequence]->count);
		
		if(array && sequence>SUDDEN+1)
			new_item_no=conditional_pattern_base(Current->itemname);  //new_item_no is the number of elements in new header table.
		else if(sequence !=0)
			new_item_no=conditional_pattern_base(Current);  //new_item_no is the number of elements in new header table.
		else
			new_item_no = 0;
		
		if(new_item_no==0 || new_item_no == 1)
		{
			if(new_item_no==1)
			{
				ITlen[list->top-1]++;
				if(fout)
					fout->printSet(list->top, list->FS, supp[order[list->FS[list->top-1]]]);
			}
			if(new_item_no==1)supp[order[list->FS[list->top-1]]] = 0;
			list->top=listlen-1;
			continue;
		}

		FI_tree *fptree;
		MB=fp_buf->bufmark(&MR, &MC);

		fptree = (FI_tree*)fp_buf->newbuf(1, sizeof(FI_tree));
		fptree->init(this->itemno, new_item_no);

		fptree->scan1_DB(this);
		fptree->scan2_DB(this, Current);

		list->top=listlen;							
		if(fptree->Single_path())
		{
                               /* patch   Oct. 9, 2003*/
                        Fnode* node;
                        for(node=fptree->Root->leftchild; node!=NULL; node=node->leftchild)
                                list->FS[list->top++] = node->itemname;
                        list->top = listlen;
                              /*************************/
			fptree->generate_all(new_item_no, fout);
			list->top--;
		}else{             //not single path
			i = fptree->FP_growth(fout);
			list->top = listlen-1;
		}
		fp_buf->freebuf(MR, MC, MB);
	}
	return 0;
}

int FI_tree::FPmax(FSout* fout)
{
	static int ms=9;		//power2[i] is the smallest block size for memory  2**9 = 512

	int i, sequence, current, new_item_no, listlen;
	int MC=0;			//markcount for memory
	unsigned int MR=0;	//markrest for memory
	char* MB;			//markbuf for memory

	Fnode* Current;

	for(sequence=itemno-1; sequence>=0; sequence--)
	{
		Current=head[sequence];
		current=head[sequence]->itemname;
		list->FS[list->top++]=head[sequence]->itemname;
		listlen = list->top;

		if(array && sequence>SUDDEN+1)
			new_item_no=conditional_pattern_base(Current->itemname);  //new_item_no is the number of elements in new header table.
		else if(sequence !=0)
			new_item_no=conditional_pattern_base(Current);  //new_item_no is the number of elements in new header table.
		else
			new_item_no = 0;
		

		if(LMaxsets->is_subset())            
		{
			for(int j=listlen; j < list->top; j++)supp[order[list->FS[j]]] = 0;

			list->top=listlen-1;			
			if(new_item_no == sequence)return new_item_no;  
			continue;
		}
		if(new_item_no==0 || new_item_no == 1)
		{
			ITlen[list->top-1]++;
			if(fout){
				if(new_item_no==1)
					fout->printSet(list->top, list->FS, supp[order[list->FS[list->top-1]]]);
				else
					fout->printSet(list->top, list->FS, this->head[sequence]->count);
			}
			LMaxsets->insert(list->FS, LMaxsets->posi+1, list->top+new_item_no-1);
			update_mfi_trees(LMaxsets->posi+1);
			list->top = listlen+1;
			if(new_item_no==1)supp[order[list->FS[list->top-1]]] = 0;
			list->top=listlen-1;
			if(new_item_no != sequence)continue;
			return new_item_no;
		}

		FI_tree *fptree;
		MB=fp_buf->bufmark(&MR, &MC);

		fptree = (FI_tree*)fp_buf->newbuf(1, sizeof(FI_tree));
		fptree->init(this->itemno, new_item_no);

		fptree->scan1_DB(this);
		fptree->scan2_DB(this, Current);

		list->top=listlen;							
		if(fptree->Single_path())
		{
			if(fout)
			{
				fout->printSet(list->top+new_item_no, list->FS, fptree->head[fptree->itemno-1]->count);
			}
			ITlen[list->top+new_item_no-1]++;
			LMaxsets->insert(list->FS, LMaxsets->posi+1, list->top+new_item_no);
			list->top = list->top+new_item_no;
			update_mfi_trees(LMaxsets->posi+1);
			list->top = listlen;
			list->top--;
			if(new_item_no == sequence)
			{
				fp_buf->freebuf(MR, MC, MB);
				return new_item_no;
			}
		}else{             //not single path
			memory* Max_buf;
			Max_buf=allocate_buf(sequence, LMaxsets->posi, ms);

			MFI_tree* new_LMFI = (MFI_tree*)Max_buf->newbuf(1, sizeof(MFI_tree));
			new_LMFI->init(Max_buf, fptree, LMaxsets, LMaxsets->head[sequence], list->top-1);
			fptree->set_max_tree(new_LMFI);
			mfitrees[LMaxsets->posi+2] = new_LMFI;
			i=fptree->FPmax(fout);
				
			list->top = new_LMFI->posi;
			if(Max_buf->half())ms++;
			delete Max_buf;

			if(i+1 == sequence)
			{
				fp_buf->freebuf(MR, MC, MB);
				return i+1;
			}
		}
		fp_buf->freebuf(MR, MC, MB);
	}
	return 0;   
}

int FI_tree::FPclose(FSout* fout)
{
	static int ms=9;		//power2[i] is the smallest block size for memory  2**9 = 512

	int i, sequence, current, new_item_no, listlen;
	int MC=0;			//markcount for memory
	unsigned int MR=0;	//markrest for memory
	char* MB;			//markbuf for memory

	Fnode* Current;

	for(sequence=itemno-1; sequence>=0; sequence--)
	{
		Current=head[sequence];
		current=head[sequence]->itemname;
		list->FS[list->top++]=head[sequence]->itemname;
		list->counts[list->top-1] = this->head[sequence]->count;
		listlen = list->top;

		if(LClose->is_subset(this->head[sequence]->count))
		{	
			list->top=listlen-1;
			continue;
		}
		if(array && sequence>SUDDEN+1)
			new_item_no=conditional_pattern_base(Current->itemname, 1);  //new_item_no is the number of elements in new header table.
		else if(sequence !=0)
			new_item_no=conditional_pattern_base(Current, 1);  //new_item_no is the number of elements in new header table.
		else
			new_item_no = 0;
		
		if(new_item_no==0 || new_item_no == 1)
		{
			if(new_item_no==0)
			{
				ITlen[list->top-1]++;
				if(fout)
					fout->printSet(list->top, list->FS, this->head[sequence]->count);
				LClose->insert(list->FS, LClose->posi+1, list->top+new_item_no-1, this->head[sequence]->count);
				update_cfi_trees(LClose->posi+1, this->head[sequence]->count);
			}else{
				list->top=listlen;
//Bug! Dec. 5
/*				if(LClose->generate_close(1, this->head[sequence]->count, fout))
				{	
					supp[order[list->FS[listlen]]] = 0;
					list->top=listlen-1;
					return new_item_no;	
				}else
					supp[order[list->FS[listlen]]] = 0;
*/				LClose->generate_close(1, this->head[sequence]->count, fout);
				supp[order[list->FS[listlen]]] = 0;
			}
			list->top=listlen-1;
			continue;
		}
/*		int j;
		for(j=listlen; j<new_item_no+listlen && list->counts[listlen-1]!=list->counts[j]; j++);
		if(j==new_item_no+listlen)
		{
			ITlen[listlen-1]++;
			if(fout)
				fout->printSet(listlen, list->FS, this->head[sequence]->count);
			
			LClose->insert(list->FS, LClose->posi+1, listlen, this->head[sequence]->count);
			list->top = listlen;
			update_cfi_trees(LClose->posi+1, this->head[sequence]->count);
			list->top += new_item_no;
		}
*/
		FI_tree *fptree;
		MB=fp_buf->bufmark(&MR, &MC);

		fptree = (FI_tree*)fp_buf->newbuf(1, sizeof(FI_tree));
		fptree->init(this->itemno, new_item_no);

		fptree->scan1_DB(this);
		fptree->scan2_DB(this, Current);

		list->top=listlen;							
		if(fptree->Single_path(1))
		{
			list->top=listlen;							
			if(LClose->generate_close(new_item_no, this->head[sequence]->count, fout))
			{
				if(new_item_no == sequence)
				{
					fp_buf->freebuf(MR, MC, MB);
					list->top = listlen-1;
					return new_item_no;
				}
			}					
			list->top = listlen-1;
		}else{             //not single path
                               /* patch   Oct. 9, 2003 */
                        int j;
                        for(j=listlen; j<new_item_no+listlen && list->counts[listlen-1]!=list->counts[j]; j++);
                        if(j==new_item_no+listlen)
                        {
                                ITlen[listlen-1]++;
                                if(fout)
                                        fout->printSet(listlen, list->FS, this->head[sequence]->count);

                                LClose->insert(list->FS, LClose->posi+1, listlen, this->head[sequence]->count);
                                list->top = listlen;
                                update_cfi_trees(LClose->posi+1, this->head[sequence]->count);
//                              list->top += new_item_no;
                        }
                              /***************************/
			memory* Close_buf;
			Close_buf=allocate_buf(sequence, LClose->posi, ms);

			CFI_tree* new_LClose = (CFI_tree*)Close_buf->newbuf(1, sizeof(CFI_tree));
			new_LClose->init(Close_buf, fptree, LClose, LClose->head[sequence], list->top-1);
			fptree->set_close_tree(new_LClose);
			cfitrees[LClose->posi+2] = new_LClose;
			i=fptree->FPclose(fout);
			
			list->top = new_LClose->posi;
			if(Close_buf->half())ms++;
			delete Close_buf;

		}
		fp_buf->freebuf(MR, MC, MB);
	}
	return 0;   
}

