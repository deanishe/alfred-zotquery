FasdUAS 1.101.10   ��   ��    k             l      ��  ��      Prepare Dialog Skeleton      � 	 	 2   P r e p a r e   D i a l o g   S k e l e t o n     
  
 j     �� �� 0 
scpt_front    m        �   � 
 	 t r y 
 	 	 t e l l   a p p l i c a t i o n   ( p a t h   t o   f r o n t m o s t   a p p l i c a t i o n   a s   t e x t ) 
 	      j    �� �� 0 scpt_middle    m       �   � 
 	 	 e n d   t e l l 
 	 o n   e r r o r   e r r T e x t   n u m b e r   e r r N u m 
 	 	 i f   n o t   ( e r r N u m   i s   e q u a l   t o   - 1 2 8 )   t h e n 
 	 	 	 t e l l   a p p l i c a t i o n   i d   " s e v s " 
 	      j    �� �� 0 scpt_end    m       �   ~ 
 	 	 	 e n d   t e l l 
 	 	 e l s e 
 	 	 	 r e t u r n   m i s s i n g   v a l u e 
 	 	 e n d   i f 
 	 e n d   t r y 
 	      l     ��������  ��  ��        l     ��������  ��  ��        l      ��   ��    ) # USER-INTERACTION HELPER FUNCTIONS       � ! ! F   U S E R - I N T E R A C T I O N   H E L P E R   F U N C T I O N S     " # " l     ��������  ��  ��   #  $ % $ i   	  & ' & I      �� (���� 0 display_dialog   (  )�� ) o      ���� 0 rec  ��  ��   ' k    � * *  + , + l      �� - .��   -mg Displays a dialog containing a message, one to three buttons, and optionally an icon and a �eld in which the user can enter text.
	Syntax: key || class || status
		z_text || text || required
		z_answer || text || optional
		z_hidden || boolean || optional
		z_buttons || list || optional
		z_ok || labelSpecifier || optional
		z_cancel || labelSpecifier || optional
		z_title || text || optional
		z_icon || resourceSpecifier || optional
		z_icon || iconTypeSpecifier || optional
		z_icon || fileSpecifier || optional
		z_wait || integer || optional	
	
	Result: A record containing the button clicked and text entered, if any. For example {text returned:"Cupertino", button returned:"OK"}. If the dialog does not allow text input, there is no text returned item in the returned record. If the user clicks the specified cancel button, the script fails silently. If the display dialog command specifies a giving up after value, and the dialog is dismissed due to timing out before the user clicks a button, it returns a record indicating that no button was returned and the command gave up: {button returned:"", gave up:true}.
	    . � / /�   D i s p l a y s   a   d i a l o g   c o n t a i n i n g   a   m e s s a g e ,   o n e   t o   t h r e e   b u t t o n s ,   a n d   o p t i o n a l l y   a n   i c o n   a n d   a  � e l d   i n   w h i c h   t h e   u s e r   c a n   e n t e r   t e x t . 
 	 S y n t a x :   k e y   | |   c l a s s   | |   s t a t u s 
 	 	 z _ t e x t   | |   t e x t   | |   r e q u i r e d 
 	 	 z _ a n s w e r   | |   t e x t   | |   o p t i o n a l 
 	 	 z _ h i d d e n   | |   b o o l e a n   | |   o p t i o n a l 
 	 	 z _ b u t t o n s   | |   l i s t   | |   o p t i o n a l 
 	 	 z _ o k   | |   l a b e l S p e c i f i e r   | |   o p t i o n a l 
 	 	 z _ c a n c e l   | |   l a b e l S p e c i f i e r   | |   o p t i o n a l 
 	 	 z _ t i t l e   | |   t e x t   | |   o p t i o n a l 
 	 	 z _ i c o n   | |   r e s o u r c e S p e c i f i e r   | |   o p t i o n a l 
 	 	 z _ i c o n   | |   i c o n T y p e S p e c i f i e r   | |   o p t i o n a l 
 	 	 z _ i c o n   | |   f i l e S p e c i f i e r   | |   o p t i o n a l 
 	 	 z _ w a i t   | |   i n t e g e r   | |   o p t i o n a l 	 
 	 
 	 R e s u l t :   A   r e c o r d   c o n t a i n i n g   t h e   b u t t o n   c l i c k e d   a n d   t e x t   e n t e r e d ,   i f   a n y .   F o r   e x a m p l e   { t e x t   r e t u r n e d : " C u p e r t i n o " ,   b u t t o n   r e t u r n e d : " O K " } .   I f   t h e   d i a l o g   d o e s   n o t   a l l o w   t e x t   i n p u t ,   t h e r e   i s   n o   t e x t   r e t u r n e d   i t e m   i n   t h e   r e t u r n e d   r e c o r d .   I f   t h e   u s e r   c l i c k s   t h e   s p e c i f i e d   c a n c e l   b u t t o n ,   t h e   s c r i p t   f a i l s   s i l e n t l y .   I f   t h e   d i s p l a y   d i a l o g   c o m m a n d   s p e c i f i e s   a   g i v i n g   u p   a f t e r   v a l u e ,   a n d   t h e   d i a l o g   i s   d i s m i s s e d   d u e   t o   t i m i n g   o u t   b e f o r e   t h e   u s e r   c l i c k s   a   b u t t o n ,   i t   r e t u r n s   a   r e c o r d   i n d i c a t i n g   t h a t   n o   b u t t o n   w a s   r e t u r n e d   a n d   t h e   c o m m a n d   g a v e   u p :   { b u t t o n   r e t u r n e d : " " ,   g a v e   u p : t r u e } . 
 	 ,  0 1 0 l     ��������  ��  ��   1  2 3 2 l      �� 4 5��   4 > 8 Build display dialog script, adding optional variables     5 � 6 6 p   B u i l d   d i s p l a y   d i a l o g   s c r i p t ,   a d d i n g   o p t i o n a l   v a r i a b l e s   3  7 8 7 l     �� 9 :��   9 D >The dialog text, which is displayed in emphasized system font.    : � ; ; | T h e   d i a l o g   t e x t ,   w h i c h   i s   d i s p l a y e d   i n   e m p h a s i z e d   s y s t e m   f o n t . 8  < = < r     	 > ? > b      @ A @ b      B C B m      D D � E E   d i s p l a y   d i a l o g   " C l    F���� F n     G H G o    ���� 
0 z_text   H o    ���� 0 rec  ��  ��   A m     I I � J J  " ? o      ���� 0 scpt   =  K L K l  
 
�� M N��   M � �The initial contents of an editable text field. This edit field is not present unless this parameter is present; to have the field present but blank, specify an empty string: default answer ""    N � O O� T h e   i n i t i a l   c o n t e n t s   o f   a n   e d i t a b l e   t e x t   f i e l d .   T h i s   e d i t   f i e l d   i s   n o t   p r e s e n t   u n l e s s   t h i s   p a r a m e t e r   i s   p r e s e n t ;   t o   h a v e   t h e   f i e l d   p r e s e n t   b u t   b l a n k ,   s p e c i f y   a n   e m p t y   s t r i n g :   d e f a u l t   a n s w e r   " " L  P Q P Q   
 # R S�� R r     T U T b     V W V b     X Y X b     Z [ Z b     \ ] \ o    ���� 0 scpt   ] 1    ��
�� 
spac [ m     ^ ^ � _ _   d e f a u l t   a n s w e r   " Y l    `���� ` n     a b a o    ���� 0 z_answer   b o    ���� 0 rec  ��  ��   W m     c c � d d  " U o      ���� 0 scpt   S R      ������
�� .ascrerr ****      � ****��  ��  ��   Q  e f e l  $ $�� g h��   g w qIf true, any text in the edit field is obscured as in a password dialog: each character is displayed as a bullet.    h � i i � I f   t r u e ,   a n y   t e x t   i n   t h e   e d i t   f i e l d   i s   o b s c u r e d   a s   i n   a   p a s s w o r d   d i a l o g :   e a c h   c h a r a c t e r   i s   d i s p l a y e d   a s   a   b u l l e t . f  j k j Q   $ ; l m�� l r   ' 2 n o n b   ' 0 p q p b   ' , r s r b   ' * t u t o   ' (���� 0 scpt   u 1   ( )��
�� 
spac s m   * + v v � w w  h i d d e n   a n s w e r   q l  , / x���� x n   , / y z y o   - /���� 0 z_hidden   z o   , -���� 0 rec  ��  ��   o o      ���� 0 scpt   m R      ������
�� .ascrerr ****      � ****��  ��  ��   k  { | { l  < <�� } ~��   } ) #A list of up to three button names.    ~ �   F A   l i s t   o f   u p   t o   t h r e e   b u t t o n   n a m e s . |  � � � Q   < \ � ��� � k   ? S � �  � � � r   ? I � � � n  ? G � � � I   @ G�� ����� 0 stringify_list   �  ��� � n   @ C � � � o   A C���� 0 	z_buttons   � o   @ A���� 0 rec  ��  ��   �  f   ? @ � o      ���� 0 b   �  ��� � r   J S � � � b   J Q � � � b   J O � � � b   J M � � � o   J K���� 0 scpt   � 1   K L��
�� 
spac � m   M N � � � � �  b u t t o n s   � o   O P���� 0 b   � o      ���� 0 scpt  ��   � R      ������
�� .ascrerr ****      � ****��  ��  ��   �  � � � l  ] ]�� � ���   � � �The name or number of the default button. This button is highlighted, and will be pressed if the user presses the Return or Enter key.    � � � � T h e   n a m e   o r   n u m b e r   o f   t h e   d e f a u l t   b u t t o n .   T h i s   b u t t o n   i s   h i g h l i g h t e d ,   a n d   w i l l   b e   p r e s s e d   i f   t h e   u s e r   p r e s s e s   t h e   R e t u r n   o r   E n t e r   k e y . �  � � � Q   ] � � ��� � Z   ` � � � ��� � =   ` i � � � n   ` e � � � m   c e��
�� 
pcls � n   ` c � � � o   a c���� 0 z_ok   � o   ` a���� 0 rec   � m   e h��
�� 
ctxt � r   l } � � � b   l { � � � b   l w � � � b   l s � � � b   l o � � � o   l m���� 0 scpt   � 1   m n��
�� 
spac � m   o r � � � � �   d e f a u l t   b u t t o n   " � l  s v ����� � n   s v � � � o   t v���� 0 z_ok   � o   s t���� 0 rec  ��  ��   � m   w z � � � � �  " � o      ���� 0 scpt   �  � � � =   � � � � � n   � � � � � m   � ���
�� 
pcls � n   � � � � � o   � ����� 0 z_ok   � o   � ����� 0 rec   � m   � ���
�� 
long �  ��� � r   � � � � � b   � � � � � b   � � � � � b   � � � � � o   � ����� 0 scpt   � 1   � ���
�� 
spac � m   � � � � � � �  d e f a u l t   b u t t o n   � l  � � ����� � n   � � � � � o   � ����� 0 z_ok   � o   � ����� 0 rec  ��  ��   � o      ���� 0 scpt  ��  ��   � R      ������
�� .ascrerr ****      � ****��  ��  ��   �  � � � l  � ��� � ���   � � zThe name or number of the cancel button. This button will be pressed if the user presses the Escape key or Command-period.    � � � � � T h e   n a m e   o r   n u m b e r   o f   t h e   c a n c e l   b u t t o n .   T h i s   b u t t o n   w i l l   b e   p r e s s e d   i f   t h e   u s e r   p r e s s e s   t h e   E s c a p e   k e y   o r   C o m m a n d - p e r i o d . �  � � � Q   � � � ��� � Z   � � � � ��� � =   � � � � � n   � � � � � m   � ���
�� 
pcls � n   � � � � � o   � ����� 0 z_cancel   � o   � ����� 0 rec   � m   � ���
�� 
ctxt � r   � � � � � b   � � � � � b   � � � � � b   � � � � � b   � � � � � o   � ����� 0 scpt   � 1   � ���
�� 
spac � m   � � � � � � �  c a n c e l   b u t t o n   " � l  � � ����� � n   � � � � � o   � ����� 0 z_cancel   � o   � ����� 0 rec  ��  ��   � m   � � � � � � �  " � o      ���� 0 scpt   �  � � � =   � � � � � n   � � � � � m   � ���
�� 
pcls � n   � � � � � o   � ����� 0 z_cancel   � o   � ����� 0 rec   � m   � ���
�� 
long �  ��� � r   � � �  � b   � � b   � � b   � � o   � ����� 0 scpt   1   � ���
�� 
spac m   � � �  c a n c e l   b u t t o n   l  � �	����	 n   � �

 o   � ����� 0 z_cancel   o   � ����� 0 rec  ��  ��    o      ���� 0 scpt  ��  ��   � R      ������
�� .ascrerr ****      � ****��  ��  ��   �  l  � ���    The dialog window title.    � 0 T h e   d i a l o g   w i n d o w   t i t l e .  Q   ��~ r   � b   � b   �	 b   � b   � � o   � ��}�} 0 scpt   1   � ��|
�| 
spac m   � �    w i t h   t i t l e   " l !�{�z! n  "#" o  �y�y 0 z_title  # o  �x�x 0 rec  �{  �z   m  	$$ �%%  " o      �w�w 0 scpt   R      �v�u�t
�v .ascrerr ****      � ****�u  �t  �~   &'& l �s()�s  ( ] WThe type of icon to show (either stop, note, or caution), or an alias or file specifier   ) �** � T h e   t y p e   o f   i c o n   t o   s h o w   ( e i t h e r   s t o p ,   n o t e ,   o r   c a u t i o n ) ,   o r   a n   a l i a s   o r   f i l e   s p e c i f i e r' +,+ Q  �-.�r- Z  �/012/ E  %343 l !5�q�p5 n  !676 o  !�o�o 
0 z_icon  7 o  �n�n 0 rec  �q  �p  4 m  !$88 �99  /0 k  (U:: ;<; r  (7=>= b  (5?@? b  (1ABA m  (+CC �DD  P O S I X   f i l e   "B l +0E�m�lE n  +0FGF o  ,0�k�k 
0 z_icon  G o  +,�j�j 0 rec  �m  �l  @ m  14HH �II  "   a s   a l i a s> o      �i�i 	0 icon_  < J�hJ Q  8UKL�gK k  ;LMM NON I ;@�fP�e
�f .sysodsct****        scptP o  ;<�d�d 	0 icon_  �e  O Q�cQ r  ALRSR b  AJTUT b  AHVWV b  ADXYX o  AB�b�b 0 scpt  Y 1  BC�a
�a 
spacW m  DGZZ �[[  w i t h   i c o n  U l HI\�`�_\ o  HI�^�^ 	0 icon_  �`  �_  S o      �]�] 0 scpt  �c  L R      �\�[�Z
�\ .ascrerr ****      � ****�[  �Z  �g  �h  1 ]^] E  Xa_`_ l X]a�Y�Xa n  X]bcb o  Y]�W�W 
0 z_icon  c o  XY�V�V 0 rec  �Y  �X  ` m  ]`dd �ee  :^ f�Uf k  d�gg hih r  dsjkj b  dqlml b  dmnon m  dgpp �qq  "o l glr�T�Sr n  glsts o  hl�R�R 
0 z_icon  t o  gh�Q�Q 0 rec  �T  �S  m m  mpuu �vv  "   a s   a l i a sk o      �P�P 	0 icon_  i w�Ow Q  t�xy�Nx k  w�zz {|{ I w|�M}�L
�M .sysodsct****        scpt} o  wx�K�K 	0 icon_  �L  | ~�J~ r  }�� b  }���� b  }���� b  }���� o  }~�I�I 0 scpt  � 1  ~�H
�H 
spac� m  ���� ���  w i t h   i c o n  � l ����G�F� o  ���E�E 	0 icon_  �G  �F  � o      �D�D 0 scpt  �J  y R      �C�B�A
�C .ascrerr ****      � ****�B  �A  �N  �O  �U  2 k  ���� ��� r  ����� l ����@�?� n  ����� o  ���>�> 
0 z_icon  � o  ���=�= 0 rec  �@  �?  � o      �<�< 	0 icon_  � ��;� Q  �����:� k  ���� ��� I ���9��8
�9 .sysodsct****        scpt� o  ���7�7 	0 icon_  �8  � ��6� r  ����� b  ����� b  ����� b  ����� o  ���5�5 0 scpt  � 1  ���4
�4 
spac� m  ���� ���  w i t h   i c o n  � l ����3�2� o  ���1�1 	0 icon_  �3  �2  � o      �0�0 0 scpt  �6  � R      �/�.�-
�/ .ascrerr ****      � ****�.  �-  �:  �;  . R      �,�+�*
�, .ascrerr ****      � ****�+  �*  �r  , ��� l ���)���)  � O IThe number of seconds to wait before automatically dismissing the dialog.   � ��� � T h e   n u m b e r   o f   s e c o n d s   t o   w a i t   b e f o r e   a u t o m a t i c a l l y   d i s m i s s i n g   t h e   d i a l o g .� ��� Q  �����(� r  ����� b  ����� b  ����� b  ����� o  ���'�' 0 scpt  � 1  ���&
�& 
spac� m  ���� ���   g i v i n g   u p   a f t e r  � l ����%�$� n  ����� o  ���#�# 
0 z_wait  � o  ���"�" 0 rec  �%  �$  � o      �!�! 0 scpt  � R      � ��
�  .ascrerr ****      � ****�  �  �(  � ��� l ������  �  �  � ��� l  ������  �   Run the compiled script    � ��� 2   R u n   t h e   c o m p i l e d   s c r i p t  � ��� I �����
� .sysodsct****        scpt� b  ����� b  ����� b  ����� b  ����� o  ���� 0 
scpt_front  � o  ���� 0 scpt  � o  ���� 0 scpt_middle  � o  ���� 0 scpt  � o  ���� 0 scpt_end  �  � ��� l ������  �  return scpt   � ���  r e t u r n   s c p t�   % ��� l     ����  �  �  � ��� l     ����  �  �  � ��� i    ��� I      �
��	�
 0 choose_from_list  � ��� o      �� 0 rec  �  �	  � k    I�� ��� l      ����  �93 Allows the user to choose items from a list.

	Syntax: key || class || status
		z_list || list || required
		z_title || text || optional
		z_prompt || text || optional
		z_def || list || optional
		z_ok || text || optional
		z_cancel || text || optional
		z_multiple || boolean || optional
		z_empty || boolean || optional
		
	Result: If the user clicks the OK button, returns a list of the chosen number and/or text items; if empty selection is allowed and nothing is selected, returns an empty list ({}). If the user clicks the Cancel button, returns false.
 	   � ���f   A l l o w s   t h e   u s e r   t o   c h o o s e   i t e m s   f r o m   a   l i s t . 
 
 	 S y n t a x :   k e y   | |   c l a s s   | |   s t a t u s 
 	 	 z _ l i s t   | |   l i s t   | |   r e q u i r e d 
 	 	 z _ t i t l e   | |   t e x t   | |   o p t i o n a l 
 	 	 z _ p r o m p t   | |   t e x t   | |   o p t i o n a l 
 	 	 z _ d e f   | |   l i s t   | |   o p t i o n a l 
 	 	 z _ o k   | |   t e x t   | |   o p t i o n a l 
 	 	 z _ c a n c e l   | |   t e x t   | |   o p t i o n a l 
 	 	 z _ m u l t i p l e   | |   b o o l e a n   | |   o p t i o n a l 
 	 	 z _ e m p t y   | |   b o o l e a n   | |   o p t i o n a l 
 	 	 
 	 R e s u l t :   I f   t h e   u s e r   c l i c k s   t h e   O K   b u t t o n ,   r e t u r n s   a   l i s t   o f   t h e   c h o s e n   n u m b e r   a n d / o r   t e x t   i t e m s ;   i f   e m p t y   s e l e c t i o n   i s   a l l o w e d   a n d   n o t h i n g   i s   s e l e c t e d ,   r e t u r n s   a n   e m p t y   l i s t   ( { } ) .   I f   t h e   u s e r   c l i c k s   t h e   C a n c e l   b u t t o n ,   r e t u r n s   f a l s e . 
   	� ��� l     ����  �  �  � ��� l      ����  � @ : Build choose from list script, adding optional variables    � ��� t   B u i l d   c h o o s e   f r o m   l i s t   s c r i p t ,   a d d i n g   o p t i o n a l   v a r i a b l e s  � ��� l     ����  � H BA list of numbers and/or text objects for the user to choose from.   � ��� � A   l i s t   o f   n u m b e r s   a n d / o r   t e x t   o b j e c t s   f o r   t h e   u s e r   t o   c h o o s e   f r o m .� ��� r     
��� n    ��� I    � ����  0 stringify_list  � ���� n    ��� o    ���� 
0 z_list  � o    ���� 0 rec  ��  ��  �  f     � o      ���� 0 l  � ��� r    ��� b    ��� m    �� ��� " c h o o s e   f r o m   l i s t  � o    ���� 0 l  � o      ���� 0 scpt  � � � l   ����     Title text for the dialog.    � 4 T i t l e   t e x t   f o r   t h e   d i a l o g .   Q    *�� r    !	 b    

 b     b     b     o    ���� 0 scpt   1    ��
�� 
spac m     �  w i t h   t i t l e   " l   ���� n     o    ���� 0 z_title   o    ���� 0 rec  ��  ��   m     �  "	 o      ���� 0 scpt   R      ������
�� .ascrerr ****      � ****��  ��  ��    l  + +����   / )The prompt to be displayed in the dialog.    � R T h e   p r o m p t   t o   b e   d i s p l a y e d   i n   t h e   d i a l o g .  Q   + D !��  r   . ;"#" b   . 9$%$ b   . 7&'& b   . 3()( b   . 1*+* o   . /���� 0 scpt  + 1   / 0��
�� 
spac) m   1 2,, �--  w i t h   p r o m p t   "' l  3 6.����. n   3 6/0/ o   4 6���� 0 z_prompt  0 o   3 4���� 0 rec  ��  ��  % m   7 811 �22  "# o      ���� 0 scpt  ! R      ������
�� .ascrerr ****      � ****��  ��  ��   343 l  E E��56��  5 � �A list of numbers and/or text objects to be initially selected. The list cannot include multiple items unless you also specify multiple selections allowed true. If an item in the default items list is not in the list to choose from, it is ignored.   6 �77� A   l i s t   o f   n u m b e r s   a n d / o r   t e x t   o b j e c t s   t o   b e   i n i t i a l l y   s e l e c t e d .   T h e   l i s t   c a n n o t   i n c l u d e   m u l t i p l e   i t e m s   u n l e s s   y o u   a l s o   s p e c i f y   m u l t i p l e   s e l e c t i o n s   a l l o w e d   t r u e .   I f   a n   i t e m   i n   t h e   d e f a u l t   i t e m s   l i s t   i s   n o t   i n   t h e   l i s t   t o   c h o o s e   f r o m ,   i t   i s   i g n o r e d .4 898 Q   E �:;��: Z   H �<=>��< =   H O?@? n   H MABA m   K M��
�� 
pclsB n   H KCDC o   I K���� 	0 z_def  D o   H I���� 0 rec  @ m   M N��
�� 
list= k   R fEE FGF r   R \HIH n  R ZJKJ I   S Z��L���� 0 stringify_list  L M��M n   S VNON o   T V���� 	0 z_def  O o   S T���� 0 rec  ��  ��  K  f   R SI o      ���� 0 l  G P��P r   ] fQRQ b   ] dSTS b   ] bUVU b   ] `WXW o   ] ^���� 0 scpt  X 1   ^ _��
�� 
spacV m   ` aYY �ZZ  d e f a u l t   i t e m s  T o   b c���� 0 l  R o      ���� 0 scpt  ��  > [\[ =   i r]^] n   i n_`_ m   l n��
�� 
pcls` n   i laba o   j l���� 	0 z_def  b o   i j���� 0 rec  ^ m   n q��
�� 
ctxt\ cdc r   u �efe b   u �ghg b   u �iji b   u |klk b   u xmnm o   u v���� 0 scpt  n 1   v w��
�� 
spacl m   x {oo �pp   d e f a u l t   i t e m s   { "j l  | q����q n   | rsr o   } ���� 	0 z_def  s o   | }���� 0 rec  ��  ��  h m   � �tt �uu  " }f o      ���� 0 scpt  d vwv =   � �xyx n   � �z{z m   � ���
�� 
pcls{ n   � �|}| o   � ����� 	0 z_def  } o   � ����� 0 rec  y m   � ���
�� 
longw ~��~ r   � �� b   � ���� b   � ���� b   � ���� b   � ���� b   � ���� o   � ����� 0 scpt  � 1   � ���
�� 
spac� m   � ��� ��� & d e f a u l t   i t e m s   i t e m  � l  � ������� n   � ���� o   � ����� 	0 z_def  � o   � ����� 0 rec  ��  ��  � m   � ��� ���  o f  � o   � ����� 0 l  � o      ���� 0 scpt  ��  ��  ; R      ������
�� .ascrerr ****      � ****��  ��  ��  9 ��� l  � �������  �   The name of the OK button.   � ��� 4 T h e   n a m e   o f   t h e   O K   b u t t o n .� ��� Q   � ������ r   � ���� b   � ���� b   � ���� b   � ���� b   � ���� o   � ����� 0 scpt  � 1   � ���
�� 
spac� m   � ��� ���   O K   b u t t o n   n a m e   "� l  � ������� n   � ���� o   � ����� 0 z_ok  � o   � ����� 0 rec  ��  ��  � m   � ��� ���  "� o      ���� 0 scpt  � R      ������
�� .ascrerr ****      � ****��  ��  ��  � ��� l  � �������  � $ The name of the Cancel button.   � ��� < T h e   n a m e   o f   t h e   C a n c e l   b u t t o n .� ��� Q   � ������ r   � ���� b   � ���� b   � ���� b   � ���� b   � ���� o   � ����� 0 scpt  � 1   � ���
�� 
spac� m   � ��� ��� ( c a n c e l   b u t t o n   n a m e   "� l  � ������� n   � ���� o   � ����� 0 z_cancel  � o   � ����� 0 rec  ��  ��  � m   � ��� ���  "� o      ���� 0 scpt  � R      ������
�� .ascrerr ****      � ****��  ��  ��  � ��� l  � �������  � * $Allow multiple items to be selected?   � ��� H A l l o w   m u l t i p l e   i t e m s   t o   b e   s e l e c t e d ?� ��� Q   ������ r   ���� b   ���� b   � ��� b   � ���� o   � ����� 0 scpt  � 1   � ���
�� 
spac� m   � ��� ��� 8 m u l t i p l e   s e l e c t i o n s   a l l o w e d  � l  ������ n   ��� o  ���� 0 
z_multiple  � o   ���� 0 rec  ��  ��  � o      ���� 0 scpt  � R      ������
�� .ascrerr ****      � ****��  ��  ��  � ��� l ������  � � �Allow the user to choose OK with no items selected? If false, the OK button will not be enabled unless at least one item is selected.   � ���
 A l l o w   t h e   u s e r   t o   c h o o s e   O K   w i t h   n o   i t e m s   s e l e c t e d ?   I f   f a l s e ,   t h e   O K   b u t t o n   w i l l   n o t   b e   e n a b l e d   u n l e s s   a t   l e a s t   o n e   i t e m   i s   s e l e c t e d .� ��� Q  -����� r  $��� b  "��� b  ��� b  ��� o  ���� 0 scpt  � 1  ��
�� 
spac� m  �� ��� 0 e m p t y   s e l e c t i o n   a l l o w e d  � l !������ n  !��� o  !���� 0 z_empty  � o  ���� 0 rec  ��  ��  � o      ���� 0 scpt  � R      ������
�� .ascrerr ****      � ****��  ��  ��  � ��� l ..��������  ��  ��  � ��� l  ..������  �   Run the compiled script    � ��� 2   R u n   t h e   c o m p i l e d   s c r i p t  � ��� I .G���~
� .sysodsct****        scpt� b  .C��� b  .=��� b  .;   b  .5 o  .3�}�} 0 
scpt_front   o  34�|�| 0 scpt   o  5:�{�{ 0 scpt_middle  � o  ;<�z�z 0 scpt  � o  =B�y�y 0 scpt_end  �~  � �x l HH�w�w    return scpt    �  r e t u r n   s c p t�x  � 	 l     �v�u�t�v  �u  �t  	 

 l     �s�r�q�s  �r  �q    i     I      �p�o�p 0 choose_file   �n o      �m�m 0 rec  �n  �o   k    �  l      �l�l  �� Allows the user to choose a file.

	Syntax: key || class || status
		z_prompt || text || optional
		z_types || list of text || optional
		z_def || alias || optional
		z_invisibles || boolean || optional
		z_multiple || boolean || optional
		z_package || boolean || optional
		
	Result: The selected file, as an alias. If multiple selections are allowed, returns a list containing one alias for each selected file, if any. If the user clicks the cancel button, the script fails silently.
	    ��   A l l o w s   t h e   u s e r   t o   c h o o s e   a   f i l e . 
 
 	 S y n t a x :   k e y   | |   c l a s s   | |   s t a t u s 
 	 	 z _ p r o m p t   | |   t e x t   | |   o p t i o n a l 
 	 	 z _ t y p e s   | |   l i s t   o f   t e x t   | |   o p t i o n a l 
 	 	 z _ d e f   | |   a l i a s   | |   o p t i o n a l 
 	 	 z _ i n v i s i b l e s   | |   b o o l e a n   | |   o p t i o n a l 
 	 	 z _ m u l t i p l e   | |   b o o l e a n   | |   o p t i o n a l 
 	 	 z _ p a c k a g e   | |   b o o l e a n   | |   o p t i o n a l 
 	 	 
 	 R e s u l t :   T h e   s e l e c t e d   f i l e ,   a s   a n   a l i a s .   I f   m u l t i p l e   s e l e c t i o n s   a r e   a l l o w e d ,   r e t u r n s   a   l i s t   c o n t a i n i n g   o n e   a l i a s   f o r   e a c h   s e l e c t e d   f i l e ,   i f   a n y .   I f   t h e   u s e r   c l i c k s   t h e   c a n c e l   b u t t o n ,   t h e   s c r i p t   f a i l s   s i l e n t l y . 
 	  l     �k�j�i�k  �j  �i    l      �h�h   @ : Build choose from list script, adding optional variables     � t   B u i l d   c h o o s e   f r o m   l i s t   s c r i p t ,   a d d i n g   o p t i o n a l   v a r i a b l e s     r     !"! m     ## �$$  c h o o s e   f i l e" o      �g�g 0 scpt    %&% l   �f'(�f  ' / )The prompt to be displayed in the dialog.   ( �)) R T h e   p r o m p t   t o   b e   d i s p l a y e d   i n   t h e   d i a l o g .& *+* Q    ,-�e, r    ./. b    010 b    232 b    454 b    
676 o    �d�d 0 scpt  7 1    	�c
�c 
spac5 m   
 88 �99  w i t h   p r o m p t   "3 l   :�b�a: n    ;<; o    �`�` 0 z_prompt  < o    �_�_ 0 rec  �b  �a  1 m    == �>>  "/ o      �^�^ 0 scpt  - R      �]�\�[
�] .ascrerr ****      � ****�\  �[  �e  + ?@? l   �ZAB�Z  A
A list of Uniform Type Identifiers (UTIs); for example, {"public.html", "public.rtf"}. Only files of the specified types will be selectable. For a list of system-defined UTIs, see Uniform Type Identifiers Overview. To get the UTI for a particular file, use info for.   B �CC A   l i s t   o f   U n i f o r m   T y p e   I d e n t i f i e r s   ( U T I s ) ;   f o r   e x a m p l e ,   { " p u b l i c . h t m l " ,   " p u b l i c . r t f " } .   O n l y   f i l e s   o f   t h e   s p e c i f i e d   t y p e s   w i l l   b e   s e l e c t a b l e .   F o r   a   l i s t   o f   s y s t e m - d e f i n e d   U T I s ,   s e e   U n i f o r m   T y p e   I d e n t i f i e r s   O v e r v i e w .   T o   g e t   t h e   U T I   f o r   a   p a r t i c u l a r   f i l e ,   u s e   i n f o   f o r .@ DED Q    >FG�YF k   ! 5HH IJI r   ! +KLK n  ! )MNM I   " )�XO�W�X 0 stringify_list  O P�VP n   " %QRQ o   # %�U�U 0 z_types  R o   " #�T�T 0 rec  �V  �W  N  f   ! "L o      �S�S 0 l  J S�RS r   , 5TUT b   , 3VWV b   , 1XYX b   , /Z[Z o   , -�Q�Q 0 scpt  [ 1   - .�P
�P 
spacY m   / 0\\ �]]  o f   t y p e  W o   1 2�O�O 0 l  U o      �N�N 0 scpt  �R  G R      �M�L�K
�M .ascrerr ****      � ****�L  �K  �Y  E ^_^ l  ? ?�J`a�J  ` &  The folder to begin browsing in.   a �bb @ T h e   f o l d e r   t o   b e g i n   b r o w s i n g   i n ._ cdc Q   ? �ef�Ie Z   B �ghijg E   B Gklk l  B Em�H�Gm n   B Enon o   C E�F�F 	0 z_def  o o   B C�E�E 0 rec  �H  �G  l m   E Fpp �qq  /h k   J orr sts r   J Suvu b   J Qwxw b   J Oyzy m   J K{{ �||  P O S I X   f i l e   "z l  K N}�D�C} n   K N~~ o   L N�B�B 	0 z_def   o   K L�A�A 0 rec  �D  �C  x m   O P�� ���  "   a s   a l i a sv o      �@�@ 0 def_  t ��?� Q   T o���>� k   W f�� ��� I  W \�=��<
�= .sysodsct****        scpt� o   W X�;�; 0 def_  �<  � ��:� r   ] f��� b   ] d��� b   ] b��� b   ] `��� o   ] ^�9�9 0 scpt  � 1   ^ _�8
�8 
spac� m   ` a�� ��� " d e f a u l t   l o c a t i o n  � l  b c��7�6� o   b c�5�5 0 def_  �7  �6  � o      �4�4 0 scpt  �:  � R      �3�2�1
�3 .ascrerr ****      � ****�2  �1  �>  �?  i ��� E   r y��� l  r u��0�/� n   r u��� o   s u�.�. 	0 z_def  � o   r s�-�- 0 rec  �0  �/  � m   u x�� ���  :� ��,� k   | ��� ��� r   | ���� b   | ���� b   | ���� m   | �� ���  "� l   ���+�*� n    ���� o   � ��)�) 	0 z_def  � o    ��(�( 0 rec  �+  �*  � m   � ��� ���  "   a s   a l i a s� o      �'�' 0 def_  � ��&� Q   � ����%� k   � ��� ��� I  � ��$��#
�$ .sysodsct****        scpt� o   � ��"�" 0 def_  �#  � ��!� r   � ���� b   � ���� b   � ���� b   � ���� o   � �� �  0 scpt  � 1   � ��
� 
spac� m   � ��� ��� " d e f a u l t   l o c a t i o n  � l  � ����� o   � ��� 0 def_  �  �  � o      �� 0 scpt  �!  � R      ���
� .ascrerr ****      � ****�  �  �%  �&  �,  j k   � ��� ��� r   � ���� l  � ����� n   � ���� o   � ��� 	0 z_def  � o   � ��� 0 rec  �  �  � o      �� 0 def_  � ��� Q   � ����� k   � ��� ��� I  � ����
� .sysodsct****        scpt� o   � ��� 0 def_  �  � ��� r   � ���� b   � ���� b   � ���� b   � ���� o   � ��� 0 scpt  � 1   � ��
� 
spac� m   � ��� ��� " d e f a u l t   l o c a t i o n  � l  � ���
�	� o   � ��� 0 def_  �
  �	  � o      �� 0 scpt  �  � R      ���
� .ascrerr ****      � ****�  �  �  �  f R      ���
� .ascrerr ****      � ****�  �  �I  d ��� l  � �� ���   � ' !Show invisible files and folders?   � ��� B S h o w   i n v i s i b l e   f i l e s   a n d   f o l d e r s ?� ��� Q   � ������ r   � ���� b   � ���� b   � ���� b   � ���� o   � ����� 0 scpt  � 1   � ���
�� 
spac� m   � ��� ���  i n v i s i b l e s  � l  � ������� n   � ���� o   � ����� 0 z_invisibles  � o   � ����� 0 rec  ��  ��  � o      ���� 0 scpt  � R      ������
�� .ascrerr ****      � ****��  ��  ��  � ��� l  � �������  � ~ xAllow multiple items to be selected? If true, the results will be returned in a list, even if there is exactly one item.   � ��� � A l l o w   m u l t i p l e   i t e m s   t o   b e   s e l e c t e d ?   I f   t r u e ,   t h e   r e s u l t s   w i l l   b e   r e t u r n e d   i n   a   l i s t ,   e v e n   i f   t h e r e   i s   e x a c t l y   o n e   i t e m .� ��� Q   ������ r   ���� b   ���� b   � �   b   � � o   � ����� 0 scpt   1   � ���
�� 
spac m   � � � 8 m u l t i p l e   s e l e c t i o n s   a l l o w e d  � l  ����� n   � o   ����� 0 
z_multiple   o   � ����� 0 rec  ��  ��  � o      ���� 0 scpt  � R      ������
�� .ascrerr ****      � ****��  ��  ��  � 	
	 l ����   � �Show the contents of packages? If true, packages are treated as folders, so that the user can choose a file inside a package (such as an application).    �, S h o w   t h e   c o n t e n t s   o f   p a c k a g e s ?   I f   t r u e ,   p a c k a g e s   a r e   t r e a t e d   a s   f o l d e r s ,   s o   t h a t   t h e   u s e r   c a n   c h o o s e   a   f i l e   i n s i d e   a   p a c k a g e   ( s u c h   a s   a n   a p p l i c a t i o n ) .
  Q  *�� r  ! b   b   b   o  ���� 0 scpt   1  ��
�� 
spac m   � 2 s h o w i n g   p a c k a g e   c o n t e n t s   l ���� n   o  ���� 0 	z_package   o  ���� 0 rec  ��  ��   o      ���� 0 scpt   R      ������
�� .ascrerr ****      � ****��  ��  ��     l ++��������  ��  ��    !"! l  ++��#$��  #   Run the compiled script    $ �%% 2   R u n   t h e   c o m p i l e d   s c r i p t  " &'& r  +F()( l +D*����* I +D��+��
�� .sysodsct****        scpt+ b  +@,-, b  +:./. b  +8010 b  +2232 o  +0���� 0 
scpt_front  3 o  01���� 0 scpt  1 o  27���� 0 scpt_middle  / o  89���� 0 scpt  - o  :?���� 0 scpt_end  ��  ��  ��  ) o      ���� 0 res  ' 4��4 Q  G�5675 Z  J�89��:8 ?  JQ;<; l JO=����= I JO��>��
�� .corecnte****       ****> o  JK���� 0 res  ��  ��  ��  < m  OP���� 9 k  T|?? @A@ r  TXBCB J  TV����  C o      ���� 0 l  A DED Y  YyF��GH��F s  gtIJI l gqK����K n  gqLML 1  mq��
�� 
psxpM l gmN����N n  gmOPO 4  hm��Q
�� 
cobjQ o  kl���� 0 i  P o  gh���� 0 res  ��  ��  ��  ��  J n      RSR  ;  rsS o  qr���� 0 l  �� 0 i  G m  \]���� H I ]b��T��
�� .corecnte****       ****T o  ]^���� 0 res  ��  ��  E U��U L  z|VV o  z{���� 0 l  ��  ��  : L  �WW n  �XYX 1  ����
�� 
psxpY o  ����� 0 res  6 R      ������
�� .ascrerr ****      � ****��  ��  7 L  ��ZZ n  ��[\[ 1  ����
�� 
psxp\ o  ������ 0 res  ��   ]^] l     ��������  ��  ��  ^ _`_ l     ��������  ��  ��  ` aba i    cdc I      ��e���� 0 display_notification  e f��f o      ���� 0 rec  ��  ��  d k     sgg hih l      ��jk��  jB< 
	Posts a notification using the Notification Center, containing a title, subtitle, and explanation, and optionally playing a sound.

	Syntax: key || class || status
		z_notification || text || required
		z_title || text || optional
		z_subtitle || text || optional
		z_sound || text || optional
		
	Result: None.
	   k �llx   
 	 P o s t s   a   n o t i f i c a t i o n   u s i n g   t h e   N o t i f i c a t i o n   C e n t e r ,   c o n t a i n i n g   a   t i t l e ,   s u b t i t l e ,   a n d   e x p l a n a t i o n ,   a n d   o p t i o n a l l y   p l a y i n g   a   s o u n d . 
 
 	 S y n t a x :   k e y   | |   c l a s s   | |   s t a t u s 
 	 	 z _ n o t i f i c a t i o n   | |   t e x t   | |   r e q u i r e d 
 	 	 z _ t i t l e   | |   t e x t   | |   o p t i o n a l 
 	 	 z _ s u b t i t l e   | |   t e x t   | |   o p t i o n a l 
 	 	 z _ s o u n d   | |   t e x t   | |   o p t i o n a l 
 	 	 
 	 R e s u l t :   N o n e . 
 	i mnm l     ��������  ��  ��  n opo l      ��qr��  q @ : Build choose from list script, adding optional variables    r �ss t   B u i l d   c h o o s e   f r o m   l i s t   s c r i p t ,   a d d i n g   o p t i o n a l   v a r i a b l e s  p tut l     ��vw��  v ^ XThe body text of the notification. At least one of this and the title must be specified.   w �xx � T h e   b o d y   t e x t   o f   t h e   n o t i f i c a t i o n .   A t   l e a s t   o n e   o f   t h i s   a n d   t h e   t i t l e   m u s t   b e   s p e c i f i e d .u yzy r     	{|{ b     }~} b     � m     �� ��� , d i s p l a y   n o t i f i c a t i o n   "� l   ������ n    ��� o    ���� 0 z_notification  � o    ���� 0 rec  ��  ��  ~ m    �� ���  "| o      ���� 0 scpt  z ��� l  
 
������  � ^ XThe title of the notification. At least one of this and the body text must be specified.   � ��� � T h e   t i t l e   o f   t h e   n o t i f i c a t i o n .   A t   l e a s t   o n e   o f   t h i s   a n d   t h e   b o d y   t e x t   m u s t   b e   s p e c i f i e d .� ��� Q   
 #����� r    ��� b    ��� b    ��� b    ��� b    ��� o    ���� 0 scpt  � 1    ��
�� 
spac� m    �� ���  w i t h   t i t l e   "� l   ������ n    ��� o    ���� 0 z_title  � o    ���� 0 rec  ��  ��  � m    �� ���  "� o      ���� 0 scpt  � R      ������
�� .ascrerr ****      � ****��  ��  ��  � ��� l  $ $������  � ' !The subtitle of the notification.   � ��� B T h e   s u b t i t l e   o f   t h e   n o t i f i c a t i o n .� ��� Q   $ =����� r   ' 4��� b   ' 2��� b   ' 0��� b   ' ,��� b   ' *��� o   ' (���� 0 scpt  � 1   ( )��
�� 
spac� m   * +�� ���  s u b t i t l e   "� l  , /������ n   , /��� o   - /���� 0 
z_subtitle  � o   , -���� 0 rec  ��  ��  � m   0 1�� ���  "� o      ���� 0 scpt  � R      ������
�� .ascrerr ****      � ****��  ��  ��  � ��� l  > >����  � � ~The name of a sound to play when the notification appears. This may be the base name of any sound installed in Library/Sounds.   � ��� � T h e   n a m e   o f   a   s o u n d   t o   p l a y   w h e n   t h e   n o t i f i c a t i o n   a p p e a r s .   T h i s   m a y   b e   t h e   b a s e   n a m e   o f   a n y   s o u n d   i n s t a l l e d   i n   L i b r a r y / S o u n d s .� ��� Q   > W���~� r   A N��� b   A L��� b   A J��� b   A F��� b   A D��� o   A B�}�} 0 scpt  � 1   B C�|
�| 
spac� m   D E�� ���  s o u n d   n a m e   "� l  F I��{�z� n   F I��� o   G I�y�y 0 z_sound  � o   F G�x�x 0 rec  �{  �z  � m   J K�� ���  "� o      �w�w 0 scpt  � R      �v�u�t
�v .ascrerr ****      � ****�u  �t  �~  � ��� l  X X�s�r�q�s  �r  �q  � ��� l   X X�p���p  �   Run the compiled script    � ��� 2   R u n   t h e   c o m p i l e d   s c r i p t  � ��� I  X q�o��n
�o .sysodsct****        scpt� b   X m��� b   X g��� b   X e��� b   X _��� o   X ]�m�m 0 
scpt_front  � o   ] ^�l�l 0 scpt  � o   _ d�k�k 0 scpt_middle  � o   e f�j�j 0 scpt  � o   g l�i�i 0 scpt_end  �n  � ��h� l  r r�g���g  �  return scpt   � ���  r e t u r n   s c p t�h  b ��� l     �f�e�d�f  �e  �d  � ��� l     �c�b�a�c  �b  �a  � ��� i    ��� I      �`��_�` 0 choose_folder  � ��^� o      �]�] 0 rec  �^  �_  � k    n�� ��� l      �\���\  ���  
	Allows the user to choose a directory, such as a folder or a disk.

	Syntax: key || class || status
		z_prompt || text || optional
		z_def || alias || optional
		z_invisibles || boolean || optional
		z_multiple || boolean || optional
		z_package || boolean || optional
		
	Result: The selected directory, as an alias. If multiple selections are allowed, returns a list containing one alias for each selected directory, if any. If the user clicks the cancel button, the script fails silently.
	   � ����     
 	 A l l o w s   t h e   u s e r   t o   c h o o s e   a   d i r e c t o r y ,   s u c h   a s   a   f o l d e r   o r   a   d i s k . 
 
 	 S y n t a x :   k e y   | |   c l a s s   | |   s t a t u s 
 	 	 z _ p r o m p t   | |   t e x t   | |   o p t i o n a l 
 	 	 z _ d e f   | |   a l i a s   | |   o p t i o n a l 
 	 	 z _ i n v i s i b l e s   | |   b o o l e a n   | |   o p t i o n a l 
 	 	 z _ m u l t i p l e   | |   b o o l e a n   | |   o p t i o n a l 
 	 	 z _ p a c k a g e   | |   b o o l e a n   | |   o p t i o n a l 
 	 	 
 	 R e s u l t :   T h e   s e l e c t e d   d i r e c t o r y ,   a s   a n   a l i a s .   I f   m u l t i p l e   s e l e c t i o n s   a r e   a l l o w e d ,   r e t u r n s   a   l i s t   c o n t a i n i n g   o n e   a l i a s   f o r   e a c h   s e l e c t e d   d i r e c t o r y ,   i f   a n y .   I f   t h e   u s e r   c l i c k s   t h e   c a n c e l   b u t t o n ,   t h e   s c r i p t   f a i l s   s i l e n t l y . 
 	� ��� l     �[�Z�Y�[  �Z  �Y  � ��� l      �X �X    @ : Build choose from list script, adding optional variables     � t   B u i l d   c h o o s e   f r o m   l i s t   s c r i p t ,   a d d i n g   o p t i o n a l   v a r i a b l e s  �  r      m      �  c h o o s e   f o l d e r o      �W�W 0 scpt   	
	 l   �V�V   / )The prompt to be displayed in the dialog.    � R T h e   p r o m p t   t o   b e   d i s p l a y e d   i n   t h e   d i a l o g .
  Q    �U r     b     b     b     b    
 o    �T�T 0 scpt   1    	�S
�S 
spac m   
  �  w i t h   p r o m p t   " l   �R�Q n      o    �P�P 0 z_prompt    o    �O�O 0 rec  �R  �Q   m    !! �""  " o      �N�N 0 scpt   R      �M�L�K
�M .ascrerr ****      � ****�L  �K  �U   #$# l   �J%&�J  % &  The folder to begin browsing in.   & �'' @ T h e   f o l d e r   t o   b e g i n   b r o w s i n g   i n .$ ()( Q    �*+�I* Z   ! �,-./, E   ! &010 l  ! $2�H�G2 n   ! $343 o   " $�F�F 	0 z_def  4 o   ! "�E�E 0 rec  �H  �G  1 m   $ %55 �66  /- k   ) N77 898 r   ) 2:;: b   ) 0<=< b   ) .>?> m   ) *@@ �AA  P O S I X   f i l e   "? l  * -B�D�CB n   * -CDC o   + -�B�B 	0 z_def  D o   * +�A�A 0 rec  �D  �C  = m   . /EE �FF  "   a s   a l i a s; o      �@�@ 0 def_  9 G�?G Q   3 NHI�>H k   6 EJJ KLK I  6 ;�=M�<
�= .sysodsct****        scptM o   6 7�;�; 0 def_  �<  L N�:N r   < EOPO b   < CQRQ b   < ASTS b   < ?UVU o   < =�9�9 0 scpt  V 1   = >�8
�8 
spacT m   ? @WW �XX " d e f a u l t   l o c a t i o n  R l  A BY�7�6Y o   A B�5�5 0 def_  �7  �6  P o      �4�4 0 scpt  �:  I R      �3�2�1
�3 .ascrerr ****      � ****�2  �1  �>  �?  . Z[Z E   Q V\]\ l  Q T^�0�/^ n   Q T_`_ o   R T�.�. 	0 z_def  ` o   Q R�-�- 0 rec  �0  �/  ] m   T Uaa �bb  :[ c�,c k   Y �dd efe r   Y bghg b   Y `iji b   Y ^klk m   Y Zmm �nn  "l l  Z ]o�+�*o n   Z ]pqp o   [ ]�)�) 	0 z_def  q o   Z [�(�( 0 rec  �+  �*  j m   ^ _rr �ss  "   a s   a l i a sh o      �'�' 0 def_  f t�&t Q   c �uv�%u k   f www xyx I  f k�$z�#
�$ .sysodsct****        scptz o   f g�"�" 0 def_  �#  y {�!{ r   l w|}| b   l u~~ b   l s��� b   l o��� o   l m� �  0 scpt  � 1   m n�
� 
spac� m   o r�� ��� " d e f a u l t   l o c a t i o n   l  s t���� o   s t�� 0 def_  �  �  } o      �� 0 scpt  �!  v R      ���
� .ascrerr ****      � ****�  �  �%  �&  �,  / k   � ��� ��� r   � ���� l  � ����� n   � ���� o   � ��� 	0 z_def  � o   � ��� 0 rec  �  �  � o      �� 0 def_  � ��� Q   � ����� k   � ��� ��� I  � ����
� .sysodsct****        scpt� o   � ��� 0 def_  �  � ��� r   � ���� b   � ���� b   � ���� b   � ���� o   � ��� 0 scpt  � 1   � ��
� 
spac� m   � ��� ��� " d e f a u l t   l o c a t i o n  � l  � ���
�	� o   � ��� 0 def_  �
  �	  � o      �� 0 scpt  �  � R      ���
� .ascrerr ****      � ****�  �  �  �  + R      ���
� .ascrerr ****      � ****�  �  �I  ) ��� l  � �� ���   � ' !Show invisible files and folders?   � ��� B S h o w   i n v i s i b l e   f i l e s   a n d   f o l d e r s ?� ��� Q   � ������ r   � ���� b   � ���� b   � ���� b   � ���� o   � ����� 0 scpt  � 1   � ���
�� 
spac� m   � ��� ���  i n v i s i b l e s  � l  � ������� n   � ���� o   � ����� 0 z_invisibles  � o   � ����� 0 rec  ��  ��  � o      ���� 0 scpt  � R      ������
�� .ascrerr ****      � ****��  ��  ��  � ��� l  � �������  � ~ xAllow multiple items to be selected? If true, the results will be returned in a list, even if there is exactly one item.   � ��� � A l l o w   m u l t i p l e   i t e m s   t o   b e   s e l e c t e d ?   I f   t r u e ,   t h e   r e s u l t s   w i l l   b e   r e t u r n e d   i n   a   l i s t ,   e v e n   i f   t h e r e   i s   e x a c t l y   o n e   i t e m .� ��� Q   � ������ r   � ���� b   � ���� b   � ���� b   � ���� o   � ����� 0 scpt  � 1   � ���
�� 
spac� m   � ��� ��� 8 m u l t i p l e   s e l e c t i o n s   a l l o w e d  � l  � ������� n   � ���� o   � ����� 0 
z_multiple  � o   � ����� 0 rec  ��  ��  � o      ���� 0 scpt  � R      ������
�� .ascrerr ****      � ****��  ��  ��  � ��� l  � �������  � � �Show the contents of packages? If true, packages are treated as folders, so that the user can choose a file inside a package (such as an application).   � ���, S h o w   t h e   c o n t e n t s   o f   p a c k a g e s ?   I f   t r u e ,   p a c k a g e s   a r e   t r e a t e d   a s   f o l d e r s ,   s o   t h a t   t h e   u s e r   c a n   c h o o s e   a   f i l e   i n s i d e   a   p a c k a g e   ( s u c h   a s   a n   a p p l i c a t i o n ) .� ��� Q   ������ r   � ���� b   � ���� b   � ���� b   � ���� o   � ����� 0 scpt  � 1   � ���
�� 
spac� m   � ��� ��� 2 s h o w i n g   p a c k a g e   c o n t e n t s  � l  � ������� n   � ���� o   � ����� 0 	z_package  � o   � ����� 0 rec  ��  ��  � o      ���� 0 scpt  � R      ������
�� .ascrerr ****      � ****��  ��  ��  � ��� l ��������  ��  ��  � ��� l  ������  �   Run the compiled script    � ��� 2   R u n   t h e   c o m p i l e d   s c r i p t  � ��� r  ��� l ������ I �����
�� .sysodsct****        scpt� b  ��� b  ��� b  ��� b  ��� o  	���� 0 
scpt_front  � o  	
���� 0 scpt  � o  ���� 0 scpt_middle  � o  ���� 0 scpt  � o  ���� 0 scpt_end  ��  ��  ��  � o      ���� 0 res  � ���� Q   n���� Z  #_������ ?  #*   l #(���� I #(����
�� .corecnte****       **** o  #$���� 0 res  ��  ��  ��   m  ()���� � k  -U  r  -1 J  -/����   o      ���� 0 l   	
	 Y  2R���� s  @M l @J���� n  @J 1  FJ��
�� 
psxp l @F���� n  @F 4  AF��
�� 
cobj o  DE���� 0 i   o  @A���� 0 res  ��  ��  ��  ��   n        ;  KL o  JK���� 0 l  �� 0 i   m  56����  I 6;����
�� .corecnte****       **** o  67���� 0 res  ��  ��  
 �� L  SU o  ST���� 0 l  ��  ��  � L  X_ n  X^ 1  Y]��
�� 
psxp o  XY���� 0 res  � R      ������
�� .ascrerr ****      � ****��  ��  � L  gn n  gm !  1  hl��
�� 
psxp! o  gh���� 0 res  ��  � "#" l     ��������  ��  ��  # $%$ l     ��������  ��  ��  % &'& i     ()( I      ��*���� 0 display_alert  * +��+ o      ���� 0 rec  ��  ��  ) k    0,, -.- l      ��/0��  /�  
	Displays a standardized alert containing a message, explanation, and from one to three buttons.

	Syntax: key || class || status
		z_display || text || required
		z_message || text || optional
		z_as || alertType || optional
		z_buttons || list || optional
		z_ok || buttonSpecifier || optional
		z_cancel || buttonSpecifier || optional
		z_wait || integer	 || optional
		
	Result: If the user clicks a button that was not specified as the cancel button, display alert returns a record that identifies the button that was clicked�for example, {button returned: "OK"}. If the command specifies a giving up after value, the record will also contain a gave up:false item. If the display alert command specifies a giving up after value, and the dialog is dismissed due to timing out before the user clicks a button, the command returns a record indicating that no button was returned and the command gave up: {button returned:"", gave up:true} If the user clicks the specified cancel button, the script fails silently.
	   0 �11�     
 	 D i s p l a y s   a   s t a n d a r d i z e d   a l e r t   c o n t a i n i n g   a   m e s s a g e ,   e x p l a n a t i o n ,   a n d   f r o m   o n e   t o   t h r e e   b u t t o n s . 
 
 	 S y n t a x :   k e y   | |   c l a s s   | |   s t a t u s 
 	 	 z _ d i s p l a y   | |   t e x t   | |   r e q u i r e d 
 	 	 z _ m e s s a g e   | |   t e x t   | |   o p t i o n a l 
 	 	 z _ a s   | |   a l e r t T y p e   | |   o p t i o n a l 
 	 	 z _ b u t t o n s   | |   l i s t   | |   o p t i o n a l 
 	 	 z _ o k   | |   b u t t o n S p e c i f i e r   | |   o p t i o n a l 
 	 	 z _ c a n c e l   | |   b u t t o n S p e c i f i e r   | |   o p t i o n a l 
 	 	 z _ w a i t   | |   i n t e g e r 	   | |   o p t i o n a l 
 	 	 
 	 R e s u l t :   I f   t h e   u s e r   c l i c k s   a   b u t t o n   t h a t   w a s   n o t   s p e c i f i e d   a s   t h e   c a n c e l   b u t t o n ,   d i s p l a y   a l e r t   r e t u r n s   a   r e c o r d   t h a t   i d e n t i f i e s   t h e   b u t t o n   t h a t   w a s   c l i c k e d  f o r   e x a m p l e ,   { b u t t o n   r e t u r n e d :   " O K " } .   I f   t h e   c o m m a n d   s p e c i f i e s   a   g i v i n g   u p   a f t e r   v a l u e ,   t h e   r e c o r d   w i l l   a l s o   c o n t a i n   a   g a v e   u p : f a l s e   i t e m .   I f   t h e   d i s p l a y   a l e r t   c o m m a n d   s p e c i f i e s   a   g i v i n g   u p   a f t e r   v a l u e ,   a n d   t h e   d i a l o g   i s   d i s m i s s e d   d u e   t o   t i m i n g   o u t   b e f o r e   t h e   u s e r   c l i c k s   a   b u t t o n ,   t h e   c o m m a n d   r e t u r n s   a   r e c o r d   i n d i c a t i n g   t h a t   n o   b u t t o n   w a s   r e t u r n e d   a n d   t h e   c o m m a n d   g a v e   u p :   { b u t t o n   r e t u r n e d : " " ,   g a v e   u p : t r u e }   I f   t h e   u s e r   c l i c k s   t h e   s p e c i f i e d   c a n c e l   b u t t o n ,   t h e   s c r i p t   f a i l s   s i l e n t l y . 
 	. 232 l     ��������  ��  ��  3 454 l      ��67��  6 @ : Build choose from list script, adding optional variables    7 �88 t   B u i l d   c h o o s e   f r o m   l i s t   s c r i p t ,   a d d i n g   o p t i o n a l   v a r i a b l e s  5 9:9 l     ��;<��  ; C =The alert text, which is displayed in emphasized system font.   < �== z T h e   a l e r t   t e x t ,   w h i c h   i s   d i s p l a y e d   i n   e m p h a s i z e d   s y s t e m   f o n t .: >?> r     	@A@ b     BCB b     DED m     FF �GG  d i s p l a y   a l e r t   "E l   H����H n    IJI o    ���� 0 	z_display  J o    ���� 0 rec  ��  ��  C m    KK �LL  "A o      ���� 0 scpt  ? MNM l  
 
��OP��  O \ VAn explanatory message, which is displayed in small system font, below the alert text.   P �QQ � A n   e x p l a n a t o r y   m e s s a g e ,   w h i c h   i s   d i s p l a y e d   i n   s m a l l   s y s t e m   f o n t ,   b e l o w   t h e   a l e r t   t e x t .N RSR Q   
 #TU��T r    VWV b    XYX b    Z[Z b    \]\ b    ^_^ o    ���� 0 scpt  _ 1    ��
�� 
spac] m    `` �aa  m e s s a g e   "[ l   b����b n    cdc o    ���� 0 	z_message  d o    ���� 0 rec  ��  ��  Y m    ee �ff  "W o      ���� 0 scpt  U R      ������
�� .ascrerr ****      � ****��  ��  ��  S ghg l  $ $��ij��  i R LThe type of alert to show. You can specify one of the following alert types:   j �kk � T h e   t y p e   o f   a l e r t   t o   s h o w .   Y o u   c a n   s p e c i f y   o n e   o f   t h e   f o l l o w i n g   a l e r t   t y p e s :h lml l  $ $��no��  n . (informational: the standard alert dialog   o �pp P i n f o r m a t i o n a l :   t h e   s t a n d a r d   a l e r t   d i a l o gm qrq l  $ $��st��  s D >warning: the alert dialog dialog is badged with a warning icon   t �uu | w a r n i n g :   t h e   a l e r t   d i a l o g   d i a l o g   i s   b a d g e d   w i t h   a   w a r n i n g   i c o nr vwv l  $ $��xy��  x ? 9critical: currently the same as the standard alert dialog   y �zz r c r i t i c a l :   c u r r e n t l y   t h e   s a m e   a s   t h e   s t a n d a r d   a l e r t   d i a l o gw {|{ Q   $ ;}~��} r   ' 2� b   ' 0��� b   ' ,��� b   ' *��� o   ' (���� 0 scpt  � 1   ( )��
�� 
spac� m   * +�� ���  a s  � l  , /������ n   , /��� o   - /���� 0 z_as  � o   , -���� 0 rec  ��  ��  � o      ���� 0 scpt  ~ R      ��~�}
� .ascrerr ****      � ****�~  �}  ��  | ��� l  < <�|���|  ���A list of up to three button names. If you supply one name, a button with that name serves as the default and is displayed on the right side of the alert dialog. If you supply two names, two buttons are displayed on the right, with the second serving as the default button. If you supply three names, the first is displayed on the left, and the next two on the right, as in the case with two buttons.   � ���  A   l i s t   o f   u p   t o   t h r e e   b u t t o n   n a m e s .   I f   y o u   s u p p l y   o n e   n a m e ,   a   b u t t o n   w i t h   t h a t   n a m e   s e r v e s   a s   t h e   d e f a u l t   a n d   i s   d i s p l a y e d   o n   t h e   r i g h t   s i d e   o f   t h e   a l e r t   d i a l o g .   I f   y o u   s u p p l y   t w o   n a m e s ,   t w o   b u t t o n s   a r e   d i s p l a y e d   o n   t h e   r i g h t ,   w i t h   t h e   s e c o n d   s e r v i n g   a s   t h e   d e f a u l t   b u t t o n .   I f   y o u   s u p p l y   t h r e e   n a m e s ,   t h e   f i r s t   i s   d i s p l a y e d   o n   t h e   l e f t ,   a n d   t h e   n e x t   t w o   o n   t h e   r i g h t ,   a s   i n   t h e   c a s e   w i t h   t w o   b u t t o n s .� ��� Q   < \���{� k   ? S�� ��� r   ? I��� n  ? G��� I   @ G�z��y�z 0 stringify_list  � ��x� n   @ C��� o   A C�w�w 0 	z_buttons  � o   @ A�v�v 0 rec  �x  �y  �  f   ? @� o      �u�u 0 l  � ��t� r   J S��� b   J Q��� b   J O��� b   J M��� o   J K�s�s 0 scpt  � 1   K L�r
�r 
spac� m   M N�� ���  b u t t o n s  � o   O P�q�q 0 l  � o      �p�p 0 scpt  �t  � R      �o�n�m
�o .ascrerr ****      � ****�n  �m  �{  � ��� l  ] ]�l���l  � Z TThe name or number of the default button. This may be the same as the cancel button.   � ��� � T h e   n a m e   o r   n u m b e r   o f   t h e   d e f a u l t   b u t t o n .   T h i s   m a y   b e   t h e   s a m e   a s   t h e   c a n c e l   b u t t o n .� ��� Q   ] ����k� Z   ` �����j� =   ` i��� n   ` e��� m   c e�i
�i 
pcls� n   ` c��� o   a c�h�h 0 z_ok  � o   ` a�g�g 0 rec  � m   e h�f
�f 
ctxt� r   l }��� b   l {��� b   l w��� b   l s��� b   l o��� o   l m�e�e 0 scpt  � 1   m n�d
�d 
spac� m   o r�� ���   d e f a u l t   b u t t o n   "� l  s v��c�b� n   s v��� o   t v�a�a 0 z_ok  � o   s t�`�` 0 rec  �c  �b  � m   w z�� ���  "� o      �_�_ 0 scpt  � ��� =   � ���� n   � ���� m   � ��^
�^ 
pcls� n   � ���� o   � ��]�] 0 z_ok  � o   � ��\�\ 0 rec  � m   � ��[
�[ 
long� ��Z� r   � ���� b   � ���� b   � ���� b   � ���� o   � ��Y�Y 0 scpt  � 1   � ��X
�X 
spac� m   � ��� ���  d e f a u l t   b u t t o n  � l  � ���W�V� n   � ���� o   � ��U�U 0 z_ok  � o   � ��T�T 0 rec  �W  �V  � o      �S�S 0 scpt  �Z  �j  � R      �R�Q�P
�R .ascrerr ****      � ****�Q  �P  �k  � ��� l  � ��O���O  � n hThe name or number of the cancel button. See �Result� below. This may be the same as the default button.   � ��� � T h e   n a m e   o r   n u m b e r   o f   t h e   c a n c e l   b u t t o n .   S e e    R e s u l t    b e l o w .   T h i s   m a y   b e   t h e   s a m e   a s   t h e   d e f a u l t   b u t t o n .� ��� Q   � ����N� Z   � �����M� =   � ���� n   � ���� m   � ��L
�L 
pcls� n   � ���� o   � ��K�K 0 z_cancel  � o   � ��J�J 0 rec  � m   � ��I
�I 
ctxt� r   � ���� b   � ���� b   � ���� b   � ���� b   � ���� o   � ��H�H 0 scpt  � 1   � ��G
�G 
spac� m   � �   �  c a n c e l   b u t t o n   "� l  � ��F�E n   � � o   � ��D�D 0 z_cancel   o   � ��C�C 0 rec  �F  �E  � m   � � �  "� o      �B�B 0 scpt  �  =   � �	
	 n   � � m   � ��A
�A 
pcls n   � � o   � ��@�@ 0 z_cancel   o   � ��?�? 0 rec  
 m   � ��>
�> 
long �= r   � � b   � � b   � � b   � � o   � ��<�< 0 scpt   1   � ��;
�; 
spac m   � � �  c a n c e l   b u t t o n   l  � ��:�9 n   � � o   � ��8�8 0 z_cancel   o   � ��7�7 0 rec  �:  �9   o      �6�6 0 scpt  �=  �M  � R      �5�4�3
�5 .ascrerr ****      � ****�4  �3  �N  �  l  � ��2 �2   N HThe number of seconds to wait before automatically dismissing the alert.     �!! � T h e   n u m b e r   o f   s e c o n d s   t o   w a i t   b e f o r e   a u t o m a t i c a l l y   d i s m i s s i n g   t h e   a l e r t . "#" Q   �$%�1$ r   �&'& b   �	()( b   �*+* b   � �,-, o   � ��0�0 0 scpt  - 1   � ��/
�/ 
spac+ m   �.. �//   g i v i n g   u p   a f t e r  ) l 0�.�-0 n  121 o  �,�, 
0 z_wait  2 o  �+�+ 0 rec  �.  �-  ' o      �*�* 0 scpt  % R      �)�(�'
�) .ascrerr ****      � ****�(  �'  �1  # 343 l �&�%�$�&  �%  �$  4 565 l  �#78�#  7   Run the compiled script    8 �99 2   R u n   t h e   c o m p i l e d   s c r i p t  6 :;: I .�"<�!
�" .sysodsct****        scpt< b  *=>= b  $?@? b  "ABA b  CDC o  � �  0 
scpt_front  D o  �� 0 scpt  B o  !�� 0 scpt_middle  @ o  "#�� 0 scpt  > o  $)�� 0 scpt_end  �!  ; E�E l //�FG�  F  return scpt   G �HH  r e t u r n   s c p t�  ' IJI l     ����  �  �  J KLK l     ����  �  �  L MNM i   ! $OPO I      �Q�� 0 say_text  Q R�R o      �� 0 rec  �  �  P k     �SS TUT l      �VW�  V � �  
	Speaks the specified text.

	Syntax:
		z_say || text || required
		z_display || text || optional
		z_use || text || optional
		z_waiting || boolean || optional
		z_save || fileSpecifier || optional
		
	Result: None.
	   W �XX�     
 	 S p e a k s   t h e   s p e c i f i e d   t e x t . 
 
 	 S y n t a x : 
 	 	 z _ s a y   | |   t e x t   | |   r e q u i r e d 
 	 	 z _ d i s p l a y   | |   t e x t   | |   o p t i o n a l 
 	 	 z _ u s e   | |   t e x t   | |   o p t i o n a l 
 	 	 z _ w a i t i n g   | |   b o o l e a n   | |   o p t i o n a l 
 	 	 z _ s a v e   | |   f i l e S p e c i f i e r   | |   o p t i o n a l 
 	 	 
 	 R e s u l t :   N o n e . 
 	U YZY l     ����  �  �  Z [\[ l      �]^�  ] @ : Build choose from list script, adding optional variables    ^ �__ t   B u i l d   c h o o s e   f r o m   l i s t   s c r i p t ,   a d d i n g   o p t i o n a l   v a r i a b l e s  \ `a` l     �
bc�
  b  The text to speak.   c �dd $ T h e   t e x t   t o   s p e a k .a efe r     	ghg b     iji b     klk m     mm �nn 
 s a y   "l l   o�	�o n    pqp o    �� 	0 z_say  q o    �� 0 rec  �	  �  j m    rr �ss  "h o      �� 0 scpt  f tut l  
 
�vw�  v � �The text to display in the feedback window, if different from the spoken text. This parameter is ignored unless Speech Recognition is turned on (in System Preferences).   w �xxP T h e   t e x t   t o   d i s p l a y   i n   t h e   f e e d b a c k   w i n d o w ,   i f   d i f f e r e n t   f r o m   t h e   s p o k e n   t e x t .   T h i s   p a r a m e t e r   i s   i g n o r e d   u n l e s s   S p e e c h   R e c o g n i t i o n   i s   t u r n e d   o n   ( i n   S y s t e m   P r e f e r e n c e s ) .u yzy Q   
 #{|�{ r    }~} b    � b    ��� b    ��� b    ��� o    �� 0 scpt  � 1    �
� 
spac� m    �� ���  d i s p l a y i n g   "� l   �� ��� n    ��� o    ���� 0 	z_display  � o    ���� 0 rec  �   ��  � m    �� ���  "~ o      ���� 0 scpt  | R      ������
�� .ascrerr ****      � ****��  ��  �  z ��� l  $ $������  � � �The voice to speak with�for example: "Zarvox". You can use any of the voices from the System Voice pop-up on the Text to Speech tab in the Speech preferences pane.   � ���F T h e   v o i c e   t o   s p e a k   w i t h  f o r   e x a m p l e :   " Z a r v o x " .   Y o u   c a n   u s e   a n y   o f   t h e   v o i c e s   f r o m   t h e   S y s t e m   V o i c e   p o p - u p   o n   t h e   T e x t   t o   S p e e c h   t a b   i n   t h e   S p e e c h   p r e f e r e n c e s   p a n e .� ��� Q   $ =����� r   ' 4��� b   ' 2��� b   ' 0��� b   ' ,��� b   ' *��� o   ' (���� 0 scpt  � 1   ( )��
�� 
spac� m   * +�� ���  u s i n g   "� l  , /������ n   , /��� o   - /���� 	0 z_use  � o   , -���� 0 rec  ��  ��  � m   0 1�� ���  "� o      ���� 0 scpt  � R      ������
�� .ascrerr ****      � ****��  ��  ��  � ��� l  > >������  � � �Should the command wait for speech to complete before returning? This parameter is ignored unless Speech Recognition is turned on (in System Preferences).   � ���4 S h o u l d   t h e   c o m m a n d   w a i t   f o r   s p e e c h   t o   c o m p l e t e   b e f o r e   r e t u r n i n g ?   T h i s   p a r a m e t e r   i s   i g n o r e d   u n l e s s   S p e e c h   R e c o g n i t i o n   i s   t u r n e d   o n   ( i n   S y s t e m   P r e f e r e n c e s ) .� ��� Q   > U����� r   A L��� b   A J��� b   A F��� b   A D��� o   A B���� 0 scpt  � 1   B C��
�� 
spac� m   D E�� ��� 2 w a i t i n g   u n t i l   c o m p l e t i o n  � l  F I������ n   F I��� o   G I���� 0 	z_waiting  � o   F G���� 0 rec  ��  ��  � o      ���� 0 scpt  � R      ������
�� .ascrerr ****      � ****��  ��  ��  � ��� l  V V������  � � �An alias or file specifier to an AIFF file (existing or not) to contain the sound output. You can only use an alias specifier if the file exists. If this parameter is specified, the sound is not played audibly, only saved to the file.   � ���� A n   a l i a s   o r   f i l e   s p e c i f i e r   t o   a n   A I F F   f i l e   ( e x i s t i n g   o r   n o t )   t o   c o n t a i n   t h e   s o u n d   o u t p u t .   Y o u   c a n   o n l y   u s e   a n   a l i a s   s p e c i f i e r   i f   t h e   f i l e   e x i s t s .   I f   t h i s   p a r a m e t e r   i s   s p e c i f i e d ,   t h e   s o u n d   i s   n o t   p l a y e d   a u d i b l y ,   o n l y   s a v e d   t o   t h e   f i l e .� ��� Q   V ������ k   Y ��� ��� Z   Y ������ E   Y ^��� l  Y \������ n   Y \��� o   Z \���� 
0 z_save  � o   Y Z���� 0 rec  ��  ��  � m   \ ]�� ���  /� r   a n��� b   a l��� b   a h��� m   a d�� ���  P O S I X   f i l e   "� l  d g������ n   d g��� o   e g���� 
0 z_save  � o   d e���� 0 rec  ��  ��  � m   h k�� ���  "   a s   a l i a s� o      ���� 0 def_  � ��� E   q x��� l  q t������ n   q t��� o   r t���� 
0 z_save  � o   q r���� 0 rec  ��  ��  � m   t w�� ���  :� ���� r   { ���� b   { ���� b   { ���� m   { ~�� ���  "� l  ~ ������� n   ~ ���� o    ����� 
0 z_save  � o   ~ ���� 0 rec  ��  ��  � m   � ��� ���  "   a s   a l i a s� o      ���� 0 def_  ��  � r   � ���� l  � ������� n   � ���� o   � ����� 
0 z_save  � o   � ����� 0 rec  ��  ��  � o      ���� 0 def_  � ���� r   � �� � b   � � b   � � b   � � o   � ����� 0 scpt   1   � ���
�� 
spac m   � � �  s a v i n g   t o   l  � �	����	 o   � ����� 0 def_  ��  ��    o      ���� 0 scpt  ��  � R      ������
�� .ascrerr ****      � ****��  ��  ��  � 

 l  � ���������  ��  ��    l   � �����     Run the compiled script     � 2   R u n   t h e   c o m p i l e d   s c r i p t    I  � �����
�� .sysodsct****        scpt b   � � b   � � b   � � b   � � o   � ����� 0 
scpt_front   o   � ����� 0 scpt   o   � ����� 0 scpt_middle   o   � ����� 0 scpt   o   � ����� 0 scpt_end  ��   �� l  � �����    return scpt    �  r e t u r n   s c p t��  N  !  l     ��������  ��  ��  ! "#" l     ��������  ��  ��  # $%$ l      ��&'��  &  
 HANDLERS    ' �((    H A N D L E R S  % )*) l     ��������  ��  ��  * +,+ i   % (-.- I      ��/���� 0 stringify_list  / 0��0 o      ���� 0 l  ��  ��  . k     511 232 Y      4��56��4 r    787 l   9����9 b    :;: b    <=< m    >> �??  "= l   @����@ n    ABA 4    ��C
�� 
cobjC o    ���� 0 i  B o    ���� 0 l  ��  ��  ; m    DD �EE  "��  ��  8 n      FGF 4    ��H
�� 
cobjH o    ���� 0 i  G o    ���� 0 l  �� 0 i  5 m    ���� 6 I   	��I��
�� .corecnte****       ****I o    ���� 0 l  ��  ��  3 JKJ r   ! *LML n  ! (NON I   " (��P���� 0 implode  P QRQ m   " #SS �TT  ,  R U��U o   # $���� 0 l  ��  ��  O  f   ! "M o      ���� 0 l  K VWV r   + 2XYX b   + 0Z[Z b   + .\]\ m   + ,^^ �__  {] o   , -���� 0 l  [ m   . /`` �aa  }Y o      ���� 0 l  W b��b L   3 5cc o   3 4���� 0 l  ��  , ded l     ��������  ��  ��  e fgf l      ��hi��  h   SUB-ROUTINES    i �jj    S U B - R O U T I N E S  g klk l     ��������  ��  ��  l m��m i   ) ,non I      ��p��� 0 implode  p qrq o      �~�~ 0 	delimiter  r s�}s o      �|�| 
0 pieces  �}  �  o k     3tt uvu q      ww �{x�{ 0 	delimiter  x �zy�z 
0 pieces  y �y�x�y 0 astid ASTID�x  v z{z r     |}| n    ~~ 1    �w
�w 
txdl 1     �v
�v 
ascr} o      �u�u 0 astid ASTID{ ��t� Q    3���� k   	 �� ��� r   	 ��� o   	 
�s�s 0 	delimiter  � n     ��� 1    �r
�r 
txdl� 1   
 �q
�q 
ascr� ��� r    ��� b    ��� m    �� ���  � o    �p�p 
0 pieces  � o      �o�o 
0 pieces  � ��� r    ��� o    �n�n 0 astid ASTID� n     ��� 1    �m
�m 
txdl� 1    �l
�l 
ascr� ��k� l   ���� L    �� o    �j�j 
0 pieces  �  > text   � ���  >   t e x t�k  � R      �i��
�i .ascrerr ****      � ****� o      �h�h 0 emsg eMsg� �g��f
�g 
errn� o      �e�e 0 enum eNum�f  � k   % 3�� ��� r   % *��� o   % &�d�d 0 astid ASTID� n     ��� 1   ' )�c
�c 
txdl� 1   & '�b
�b 
ascr� ��a� R   + 3�`��
�` .ascrerr ****      � ****� b   / 2��� m   / 0�� ���  C a n ' t   i m p l o d e :  � o   0 1�_�_ 0 emsg eMsg� �^��]
�^ 
errn� o   - .�\�\ 0 enum eNum�]  �a  �t  ��       �[�   ����������[  � �Z�Y�X�W�V�U�T�S�R�Q�P�O�Z 0 
scpt_front  �Y 0 scpt_middle  �X 0 scpt_end  �W 0 display_dialog  �V 0 choose_from_list  �U 0 choose_file  �T 0 display_notification  �S 0 choose_folder  �R 0 display_alert  �Q 0 say_text  �P 0 stringify_list  �O 0 implode  � �N '�M�L���K�N 0 display_dialog  �M �J��J �  �I�I 0 rec  �L  � �H�G�F�E�H 0 rec  �G 0 scpt  �F 0 b  �E 	0 icon_  � ) D�D I�C ^�B c�A�@ v�?�>�= ��<�;�: � ��9 ��8 � ��7$�68CH�5Zdpu����4�D 
0 z_text  
�C 
spac�B 0 z_answer  �A  �@  �? 0 z_hidden  �> 0 	z_buttons  �= 0 stringify_list  �< 0 z_ok  
�; 
pcls
�: 
ctxt
�9 
long�8 0 z_cancel  �7 0 z_title  �6 
0 z_icon  
�5 .sysodsct****        scpt�4 
0 z_wait  �K���,%�%E�O ��%�%��,%�%E�W X  hO ��%�%��,%E�W X  hO )��,k+ E�O��%�%�%E�W X  hO B��,�,a   ��%a %��,%a %E�Y ��,�,a   ��%a %��,%E�Y hW X  hO J�a ,�,a   ��%a %�a ,%a %E�Y #�a ,�,a   ��%a %�a ,%E�Y hW X  hO ��%a %�a ,%a %E�W X  hO ��a ,a  2a �a ,%a %E�O �j  O��%a !%�%E�W X  hY c�a ,a " 2a #�a ,%a $%E�O �j  O��%a %%�%E�W X  hY '�a ,E�O �j  O��%a &%�%E�W X  hW X  hO ��%a '%�a (,%E�W X  hOb   �%b  %�%b  %j  OP� �3��2�1���0�3 0 choose_from_list  �2 �/��/ �  �.�. 0 rec  �1  � �-�,�+�- 0 rec  �, 0 l  �+ 0 scpt  � !�*�)��(�'�&�%,�$1�#�"�!Y� ot���������������* 
0 z_list  �) 0 stringify_list  
�( 
spac�' 0 z_title  �&  �%  �$ 0 z_prompt  �# 	0 z_def  
�" 
pcls
�! 
list
�  
ctxt
� 
long� 0 z_ok  � 0 z_cancel  � 0 
z_multiple  � 0 z_empty  
� .sysodsct****        scpt�0J)��,k+ E�O�%E�O ��%�%��,%�%E�W X  hO ��%�%��,%�%E�W X  hO i��,�,�  )��,k+ E�O��%�%�%E�Y E��,�,a   ��%a %��,%a %E�Y %��,�,a   ��%a %��,%a %�%E�Y hW X  hO ��%a %�a ,%a %E�W X  hO ��%a %�a ,%a %E�W X  hO ��%a %�a ,%E�W X  hO ��%a %�a ,%E�W X  hOb   �%b  %�%b  %j  OP� ������� 0 choose_file  � ��� �  �� 0 rec  �  � ������� 0 rec  � 0 scpt  � 0 l  � 0 def_  � 0 res  � 0 i  � #�8�=��
�	�\�p{��������������� 
� 
spac� 0 z_prompt  �  �
  �	 0 z_types  � 0 stringify_list  � 	0 z_def  
� .sysodsct****        scpt� 0 z_invisibles  � 0 
z_multiple  � 0 	z_package  
� .corecnte****       ****
� 
cobj
�  
psxp���E�O ��%�%��,%�%E�W X  hO )��,k+ E�O��%�%�%E�W X  hO ���,� *��,%�%E�O �j O��%�%�%E�W X  hY ]��,a  0a ��,%a %E�O �j O��%a %�%E�W X  hY %��,E�O �j O��%a %�%E�W X  hW X  hO ��%a %�a ,%E�W X  hO ��%a %�a ,%E�W X  hO ��%a %�a ,%E�W X  hOb   �%b  %�%b  %j E�O A�j k -jvE�O k�j kh �a �/a ,�6G[OY��O�Y 	�a ,EW X  �a ,E� ��d���������� 0 display_notification  �� ����� �  ���� 0 rec  ��  � ������ 0 rec  �� 0 scpt  � �������������������������� 0 z_notification  
�� 
spac�� 0 z_title  ��  ��  �� 0 
z_subtitle  �� 0 z_sound  
�� .sysodsct****        scpt�� t��,%�%E�O ��%�%��,%�%E�W X  hO ��%�%��,%�%E�W X  hO ��%�%��,%�%E�W X  hOb   �%b  %�%b  %j OP� ������������� 0 choose_folder  �� ����� �  ���� 0 rec  ��  � �������������� 0 rec  �� 0 scpt  �� 0 def_  �� 0 res  �� 0 l  �� 0 i  � ����!������5@E��Wamr�����������������
�� 
spac�� 0 z_prompt  ��  ��  �� 	0 z_def  
�� .sysodsct****        scpt�� 0 z_invisibles  �� 0 
z_multiple  �� 0 	z_package  
�� .corecnte****       ****
�� 
cobj
�� 
psxp��o�E�O ��%�%��,%�%E�W X  hO ���,� *��,%�%E�O �j O��%�%�%E�W X  hY W��,� ,��,%�%E�O �j O��%a %�%E�W X  hY %��,E�O �j O��%a %�%E�W X  hW X  hO ��%a %�a ,%E�W X  hO ��%a %�a ,%E�W X  hO ��%a %�a ,%E�W X  hOb   �%b  %�%b  %j E�O A�j k -jvE�O k�j kh �a �/a ,�6G[OY��O�Y 	�a ,EW X  �a ,E� ��)���������� 0 display_alert  �� ����� �  ���� 0 rec  ��  � �������� 0 rec  �� 0 scpt  �� 0 l  � F��K��`��e������������������������� .������ 0 	z_display  
�� 
spac�� 0 	z_message  ��  ��  �� 0 z_as  �� 0 	z_buttons  �� 0 stringify_list  �� 0 z_ok  
�� 
pcls
�� 
ctxt
�� 
long�� 0 z_cancel  �� 
0 z_wait  
�� .sysodsct****        scpt��1��,%�%E�O ��%�%��,%�%E�W X  hO ��%�%��,%E�W X  hO )��,k+ E�O��%�%�%E�W X  hO B��,�,a   ��%a %��,%a %E�Y ��,�,a   ��%a %��,%E�Y hW X  hO J�a ,�,a   ��%a %�a ,%a %E�Y #�a ,�,a   ��%a %�a ,%E�Y hW X  hO ��%a %�a ,%E�W X  hOb   �%b  %�%b  %j OP� ��P���������� 0 say_text  �� ����� �  ���� 0 rec  ��  � �������� 0 rec  �� 0 scpt  �� 0 def_  � m��r����������������������������� 	0 z_say  
�� 
spac�� 0 	z_display  ��  ��  �� 	0 z_use  �� 0 	z_waiting  �� 
0 z_save  
�� .sysodsct****        scpt�� ���,%�%E�O ��%�%��,%�%E�W X  hO ��%�%��,%�%E�W X  hO ��%�%��,%E�W X  hO H��,� a ��,%a %E�Y !��,a  a ��,%a %E�Y ��,E�O��%a %�%E�W X  hOb   �%b  %�%b  %j OP� ��.���������� 0 stringify_list  �� ����� �  ���� 0 l  ��  � ������ 0 l  �� 0 i  � ��>��DS��^`
�� .corecnte****       ****
�� 
cobj�� 0 implode  �� 6 k�j  kh ��/%�%��/F[OY��O)�l+ E�O�%�%E�O�� ��o���������� 0 implode  �� ����� �  ������ 0 	delimiter  �� 
0 pieces  ��  � ������������ 0 	delimiter  �� 
0 pieces  �� 0 astid ASTID�� 0 emsg eMsg�� 0 enum eNum� �����������
�� 
ascr
�� 
txdl�� 0 emsg eMsg� ������
�� 
errn�� 0 enum eNum��  
�� 
errn�� 4��,E�O ���,FO�%E�O���,FO�W X  ���,FO)�l�%ascr  ��ޭ